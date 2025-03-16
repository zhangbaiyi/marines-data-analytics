
def generate_analysis_prompt(file_name: str, file_type: str, area: str, value: float, time_period: str) -> str:
    """
    Generates a prompt to analyze the data with the given parameters.
    """
    prompt = f"""
    You are a data scientist working at a large tech company. 
    Analyze the following data from the file(s) {file_name}:
    - Type of data: {file_type}
    - Area to be analyzed: {area}
    - Value: ${value:,.2f}
    - Time Period: {time_period}
    
    Please provide a detailed fake analysis of the {file_name} performance for the specified area and time period. 
    Include trends, insights, and any other relevant information.
    """
    return prompt
