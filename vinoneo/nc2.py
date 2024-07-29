from neo4j import GraphDatabase

class Neo4jConnection:

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def query(self, query, parameters=None):
        with self._driver.session() as session:
            result = session.run(query, parameters)
            return [record for record in result]


def calculate_wine_production_percentages(uri, user, password):
    conn = Neo4jConnection(uri=uri, user=user, password=password)
    
    # Query to count total number of wines
    query_total_wines = """
    MATCH (v:vino)
    RETURN count(v) as total_wines
    """
    
    try:
        total_wines_result = conn.query(query_total_wines)
        total_wines = total_wines_result[0]['total_wines']
    except Exception as e:
        print(f"Error retrieving total wines count: {e}")
        conn.close()
        return
    
    # Query to count wines by country, excluding those without a country
    query_wines_by_country = """
    MATCH (v:vino)
    WHERE v.country IS NOT NULL AND v.country <> ""
    RETURN v.country as country, count(v) as wine_count
    ORDER BY wine_count DESC
    """
    
    try:
        wines_by_country_result = conn.query(query_wines_by_country)
    except Exception as e:
        print(f"Error retrieving wines by country: {e}")
        conn.close()
        return
    
    # Calculate percentages
    wine_percentages = []
    for record in wines_by_country_result:
        country = record['country']
        wine_count = record['wine_count']
        percentage = (wine_count / total_wines) * 100
        wine_percentages.append({
            'country': country,
            'wine_count': wine_count,
            'percentage': percentage
        })
    
    conn.close()
    
    # Print results
    for entry in wine_percentages:
        print(f"Country: {entry['country']}, Wine Count: {entry['wine_count']}, Percentage: {entry['percentage']:.4f}%")
    

# Connection details
uri = "bolt://localhost:7687"
user = "neo4j"
password = "toroE2024"

# Calculate and print wine production percentages
calculate_wine_production_percentages(uri, user, password)
