import pandas as pd
from CommonUtilities.custom_logger import logger

def save_basic_check_defect_file(df, path):
    location = rf"C:\Users\Anshu\Desktop\folder\ETL\ETLFramework2\DefectFiles\{path}"
    df.to_csv(location, index=False)

def save_defect_data_validation_to_file(source_query,target_query,path):
    defect_file=pd.merge(source_query,target_query,how="outer",indicator=True).query("_merge!='both'")
    defect_file['_merge']=defect_file['_merge'].replace({"left_only":"source_table","right_only":"target_table"})
    defect_file.rename(columns={"_merge":"table"},inplace=True)
    location=fr"C:\Users\Anshu\Desktop\folder\ETL\ETLFramework2\DefectFiles\{path}"
    if not defect_file.empty:
        defect_file.to_csv(location,index=False)
        logger.error(f"Mismatch records at location: {location}")
    else:
        logger.info("No mismatch recrds. Defect file was not created.")
    return defect_file


