
import json
import pandas as pd
from tqdm import tqdm

from src.scripts.utils import construct_path_from_project_root, resolve_import_path_from_project_root, generate_curr_date_to_append_to_filename
from src.utils.logging import LOGGER

def nlp_preprocess(_file_name: str) -> dict:
    excel_workbook = pd.read_excel(_file_name, sheet_name=None)
    customer_survey = {}
    for (sheet_name, df_sheet) in excel_workbook.items():
        if sheet_name == "Metadata":
            continue
        current_sheet = {}
        LOGGER.info(f"Processing Sheet: {sheet_name}")
        for index, row in tqdm(df_sheet.iterrows()):
            respondentId = row["respondentId"]
            questionLabel = row["questionLabel"]
            if respondentId not in current_sheet:
                current_sheet[respondentId] = {}
            if row['questionLabel'] and str(row['answerValues']) != 'nan':
                if questionLabel not in current_sheet[respondentId]:
                    current_sheet[respondentId][questionLabel] = row['answerValues']
            if row['questionType'] == "CPP":
                current_sheet[respondentId]['storeid'] = row['answerFreeTextValues']
                current_sheet[respondentId]['responseTime'] = row['responseTime'].strftime("%Y-%m-%d %H:%M:%S")
            elif str(row['answerFreeTextValues']) != 'nan':
                if 'answerFreeTextValues' in current_sheet[respondentId]:
                    current_sheet[respondentId]['answerFreeTextValues'] += (" " + str(row['answerFreeTextValues']))
                else:
                    current_sheet[respondentId]['answerFreeTextValues'] = str(row['answerFreeTextValues'])
        customer_survey[sheet_name] = current_sheet
    return customer_survey


if __name__ == "__main__":
    # Test
    file_name = "/Users/bz/Developer/MCCS Dataset/CustomerSurveyResponses.xlsx"
    json_data = nlp_preprocess(file_name)
    json_data = json.dumps(json_data, indent=4)
    # Save the JSON data to a file
    with open(construct_path_from_project_root("src/scripts/data_warehouse/customer_survey_responses.json"), "w") as json_file:
        json_file.write(json_data)

    # Read json
    # json_data = json.loads(construct_path_from_project_root("src/scripts/data_warehouse/customer_survey_responses.json"))
    with open(construct_path_from_project_root("src/scripts/data_warehouse/customer_survey_responses.json"), "r") as json_file:
        json_data = json.load(json_file)
    print(len(json_data))
    print(len(json_data["MainStores"]))
    print(len(json_data['MarineMarts']))
    print(len(json_data['HospitalityServices']))
    print(len(json_data['FoodBeverage']))
    # print(json_data["MainStores"]

    # LOGGER.info(f"Processed DataFrame shape: {df_customer_survey_responses.shape}")
    # df_customer_survey_responses.to_csv("processed_customer_survey_responses.csv", index=False)
    # df = pd.read_csv(r"/Users/bz/Developer/marines-data-analytics/sheet_MainStores.csv")
    # for row in df.iterrows():
    #     print(row)
    #     break
    
    
