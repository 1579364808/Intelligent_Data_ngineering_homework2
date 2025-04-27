import json
import pandas as pd # 使用 pandas 读取 JSON 更方便
from mysql.connector import connect, Error as MySQLError
from neo4j import GraphDatabase, basic_auth

# --- 配置数据库连接信息 ---
# MySQL 配置
MYSQL_CONFIG = {
    'host': '127.0.0.1', # Docker 映射的本地地址
    'port': 3306,       # Docker 映射的本地端口
    'user': 'root',
    'password': '112233445566',
    'database': 'wikidata_rdbms' # 之前创建数据库时指定的名字
}

# Neo4j 配置
NEO4J_URI = "bolt://localhost:7687" # Bolt 协议的标准端口
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "112233445566" # 你在 docker run 命令中设置的密码

# JSON 文件路径
JSON_FILE_PATH = 'wikidata_countries.json' # 确保文件名正确

# --- 函数：加载数据到 MySQL ---
def load_data_to_mysql(data):
    print("开始加载数据到 MySQL...")
    try:
        # 连接 MySQL
        with connect(**MYSQL_CONFIG) as connection:
            print("成功连接到 MySQL 数据库。")
            with connection.cursor() as cursor:
                # 1. 创建表 (如果不存在) - 使用之前设计的表结构
                print("创建 MySQL 表 (如果不存在)...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS Entities (
                        uri VARCHAR(255) PRIMARY KEY,
                        label VARCHAR(255)
                    );
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS CountryDetails (
                        country_uri VARCHAR(255) PRIMARY KEY,
                        population BIGINT,
                        area DECIMAL(15, 2),
                        area_unit VARCHAR(50),
                        FOREIGN KEY (country_uri) REFERENCES Entities(uri) ON DELETE CASCADE
                    );
                """)
                # 注意：关系类型可以更通用，或者为每种关系建表/加列
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS Relationships (
                        relationship_id INT AUTO_INCREMENT PRIMARY KEY,
                        source_uri VARCHAR(255),
                        relationship_type VARCHAR(50),
                        target_uri VARCHAR(255),
                        FOREIGN KEY (source_uri) REFERENCES Entities(uri) ON DELETE CASCADE,
                        FOREIGN KEY (target_uri) REFERENCES Entities(uri) ON DELETE CASCADE,
                        UNIQUE KEY unique_relationship (source_uri, relationship_type, target_uri) -- 防止重复关系
                    );
                """)
                connection.commit() # 提交建表操作

                # 2. 插入数据
                print("开始插入数据...")
                entities_added = set() # 用于跟踪已添加的实体，避免重复插入
                for item in data:
                    country_uri = item.get('country', {}).get('value')
                    country_label = item.get('countryLabel', {}).get('value')
                    capital_uri = item.get('capital', {}).get('value')
                    capital_label = item.get('capitalLabel', {}).get('value')
                    population = item.get('population', {}).get('value')
                    area = item.get('area', {}).get('value')
                    area_unit = item.get('areaUnitLabel', {}).get('value')

                    # 插入国家实体 (如果尚未添加)
                    if country_uri and country_uri not in entities_added:
                        cursor.execute("INSERT IGNORE INTO Entities (uri, label) VALUES (%s, %s)", (country_uri, country_label))
                        entities_added.add(country_uri)

                    # 插入首都实体 (如果存在且尚未添加)
                    if capital_uri and capital_uri not in entities_added:
                        cursor.execute("INSERT IGNORE INTO Entities (uri, label) VALUES (%s, %s)", (capital_uri, capital_label))
                        entities_added.add(capital_uri)

                    # 插入国家详情 (使用 INSERT ... ON DUPLICATE KEY UPDATE 来处理重复)
                    if country_uri:
                        try:
                            pop_val = int(population) if population else None
                            area_val = float(area) if area else None
                        except (ValueError, TypeError):
                            print(f"警告：无法转换国家 {country_label} 的人口或面积数据。Population: {population}, Area: {area}")
                            pop_val = None
                            area_val = None

                        cursor.execute("""
                            INSERT INTO CountryDetails (country_uri, population, area, area_unit)
                            VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                population = VALUES(population),
                                area = VALUES(area),
                                area_unit = VALUES(area_unit)
                        """, (country_uri, pop_val, area_val, area_unit))


                    # 插入首都关系 (如果存在)
                    if country_uri and capital_uri:
                        # 使用 INSERT IGNORE 避免因 UNIQUE KEY 约束而报错
                        cursor.execute("""
                            INSERT IGNORE INTO Relationships (source_uri, relationship_type, target_uri)
                            VALUES (%s, %s, %s)
                        """, (country_uri, 'hasCapital', capital_uri))

                connection.commit() # 提交所有插入操作
                print(f"成功将 {len(data)} 条记录（或其相关实体/关系）加载到 MySQL。")

    except MySQLError as e:
        print(f"MySQL 错误: {e}")
    except Exception as e:
        print(f"加载到 MySQL 时发生意外错误: {e}")

# --- 函数：加载数据到 Neo4j ---
def load_data_to_neo4j(data):
    print("\n开始加载数据到 Neo4j...")
    # 创建 Neo4j 驱动实例
    driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))

    # 使用 session 执行 Cypher 查询
    with driver.session(database="neo4j") as session: # 默认数据库是 neo4j
        print("成功连接到 Neo4j 数据库。")

        # 可选：添加约束以提高性能和数据一致性 (如果节点很多，建议添加)
        try:
            print("创建 Neo4j 约束 (如果不存在)...")
            session.run("CREATE CONSTRAINT country_uri_unique IF NOT EXISTS FOR (c:Country) REQUIRE c.uri IS UNIQUE")
            session.run("CREATE CONSTRAINT city_uri_unique IF NOT EXISTS FOR (c:City) REQUIRE c.uri IS UNIQUE")
            # 可以为 :Capital 添加约束，如果决定使用 :Capital 标签
        except Exception as e:
            print(f"创建 Neo4j 约束时出错 (可能已存在): {e}")


        # 准备 Cypher 语句 (使用 MERGE 避免重复)
        # 注意：参数化查询是最佳实践，可以防止 Cypher 注入并提高性能
        cypher_query = """
        // 创建或合并国家节点并设置属性
        MERGE (country:Country {uri: $country_uri})
        ON CREATE SET
            country.name = $country_label,
            country.population = $population,
            country.area = $area,
            country.area_unit = $area_unit
        ON MATCH SET // 如果节点已存在，更新属性
            country.name = $country_label,
            country.population = $population,
            country.area = $area,
            country.area_unit = $area_unit

        // 如果有首都信息，创建或合并首都节点和关系
        // 使用 FOREACH 和 CASE 模拟条件创建
        FOREACH (ignoreMe IN CASE WHEN $capital_uri IS NOT NULL THEN [1] ELSE [] END |
            MERGE (capital:City {uri: $capital_uri}) // 使用 :City 标签
            ON CREATE SET
                capital.name = $capital_label
            ON MATCH SET
                capital.name = $capital_label
            MERGE (country)-[:HAS_CAPITAL]->(capital) // 创建关系
        )
        """

        print("开始插入/更新数据...")
        count = 0
        for item in data:
            # 准备参数字典
            params = {
                'country_uri': item.get('country', {}).get('value'),
                'country_label': item.get('countryLabel', {}).get('value'),
                'capital_uri': item.get('capital', {}).get('value'),
                'capital_label': item.get('capitalLabel', {}).get('value'),
                'population': None, # 默认值
                'area': None,       # 默认值
                'area_unit': item.get('areaUnitLabel', {}).get('value')
            }

            # 安全地转换人口和面积为数值类型
            try:
                pop_str = item.get('population', {}).get('value')
                if pop_str:
                    params['population'] = int(pop_str)
            except (ValueError, TypeError):
                print(f"警告：无法转换国家 {params['country_label']} 的人口数据: {pop_str}")

            try:
                area_str = item.get('area', {}).get('value')
                if area_str:
                    params['area'] = float(area_str)
            except (ValueError, TypeError):
                print(f"警告：无法转换国家 {params['country_label']} 的面积数据: {area_str}")

            # 过滤掉没有国家 URI 的无效数据
            if not params['country_uri']:
                print(f"警告：跳过缺少 country URI 的记录: {item}")
                continue

            # 执行 Cypher 查询
            try:
                session.run(cypher_query, params)
                count += 1
            except Exception as e:
                print(f"执行 Cypher 时出错，记录: {item}, 错误: {e}")

        print(f"成功将 {count} 条记录加载/更新到 Neo4j。")

    # 关闭驱动连接
    driver.close()

# --- 主程序 ---
if __name__ == "__main__":
    # 1. 读取 JSON 数据
    try:
        # 使用 pandas 读取 JSON，更健壮
        df = pd.read_json(JSON_FILE_PATH)
        # 提取 'results' -> 'bindings' 部分的数据
        records = df['results']['bindings']
        print(f"成功从 {JSON_FILE_PATH} 读取 {len(records)} 条记录。")

        # 2. 加载到 MySQL
        load_data_to_mysql(records)

        # 3. 加载到 Neo4j
        load_data_to_neo4j(records)

        print("\n数据加载完成！")

    except FileNotFoundError:
        print(f"错误：找不到 JSON 文件 {JSON_FILE_PATH}")
    except KeyError as e:
        print(f"错误：JSON 文件结构不符合预期，缺少键: {e}")
        print("请确保 JSON 文件包含 'results' 和 'bindings' 键。")
    except Exception as e:
        print(f"处理 JSON 文件或加载数据时发生意外错误: {e}")