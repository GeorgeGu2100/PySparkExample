from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark.sql.types import *
from pyspark.sql.functions import udf

# get the commodity_score for AMI 
# based on counts of 'Yes' over a list of columns
# example input: args = ('HistoryofPTCA','HistoryofCABG')
def commodity_score_ami_fun(*args):
    value = 0
    for arg in args:
        if(arg.lower() == 'yes'):
            value += 1
    return value   

# lace score of AMI for a certain row


# score for a certain measure
# (LengthOfStay,EmergencyAdmission,Commodity_Score_AMI,Commodity_Score_AMI, EDVisit)
def lace_score_ami_fun(LengthOfStay,Inpatient_visits,Commodity_Score_AMI,ED_visits): 
    value = 0
    
    # add points due to LengthOfStay
    try:
        LengthOfStay_int = int(LengthOfStay)
    except:
        LengthOfStay_int = 0
    if(LengthOfStay_int<1):
        value += 0
    elif(LengthOfStay_int>=4 and LengthOfStay_int<=6):
        value +=  4
    elif(LengthOfStay_int>=7 and LengthOfStay_int<=13):
        value +=  5
    elif(LengthOfStay_int>=14):
        value +=  7
    else:
        value += LengthOfStay_int
    
    # Add points due to Inpatient_visits # replace EmergencyAdmission by Inpatient_visits
    if(Inpatient_visits.lower() == 'yes'):
        value += 3

    # Add points due to Commodity_Score_AMI  
    Commodity_Score_AMI_int = int(Commodity_Score_AMI)
    if(Commodity_Score_AMI_int>=4):
        value += 5
    else:
        value += Commodity_Score_AMI_int

    # add points due to LengthOfStay
    try:
        ED_visits_int = int(ED_visits)
    except:
        ED_visits_int = 0
    if(ED_visits_int>=4):
        value += 4
    else:
        value += ED_visits_int

    return value     


if __name__ == "__main__":
    # Create a SparkSession (the config bit is only for Windows!)
    print('Start Data Analysis')
    spark = SparkSession.builder.appName("Sample Data Analysis").getOrCreate()

    # Get the raw data
    # lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/SampleDataAnalysis/Sample_med_data_2016.csv")
    Dataset = spark.read.option("header","true").csv("hdfs:///user/maria_dev/SampleDataAnalysis/Sample_med_data_2016.csv") # DataFrame
    AMI_diagnosis_code_list=['410.00', '410.01', '410.10', '410.11', '410.20', '410.21', '410.30', '410.31', '410.40', '410.41', '410.50', \
    '410.51', '410.60', '410.61', '410.70', '410.71', '410.80', '410.81', '410.90', '410.91']
    AMI_column_list = ('HistoryofPTCA', 'HistoryofCABG', 'Congestiveheartfailure', 'Acutecoronarysyndrome', 'Anteriormyocardialinfarction', 'Otherlocationofmyocardialinfarction', 
    'Anginapectorisoldmyocardialinfarction', 'Coronaryatherosclerosis', 'Valvularorrheumaticheartdisease', 'Specifiedarrhythmias', 'Historyofinfection', 
    'Metastaticcanceroracuteleukemia', 'Cancer', 'Diabetesmellitus(DM)orDMcomplications', 'Protein-caloriemalnutrition', 'Disordersoffluidelectrolyteacid-base', 
    'Irondeficiencyorotheranemiasandblooddisease', 'Dementiaorotherspecifiedbraindisorders', 'Hemiplegiaparaplegiaparalysisfunctionaldisability', 'Stroke', 'Cerebrovasculardisease',
    'Vascularorcirculatorydisease', 'Chronicobstructivepulmonarydisease', 'Asthma', 'Pneumonia', 'End-stagerenaldiseaseordialysis', 'Renalfailure', 
    'Otherurinarytractdisorders', 'Decubitusulcerorchronicskinulcer' )
    
 
    # register into pyspark udf
    # an example of udf
    # fun1_udf = udf(lambda z: fun1(z), StringType()) 
    commodity_score_ami_udf = udf(commodity_score_ami_fun, IntegerType())
    # lace_score_ami_fun(LengthOfStay,EmergencyAdmission,Commodity_Score_AMI,EDVisit)
    lace_score_ami_udf = udf(lace_score_ami_fun, IntegerType())
    
    
    print('data loaded')
    # print(Dataset.printSchema())
    # print(Dataset.show(2,truncate = True))
    # print(Dataset.filter(Dataset.diagnosis_code.isin(AMI_diagnosis_code_list)).select("encounter_id","age","diagnosis_code","Protein-caloriemalnutrition","OtherEndocrine/Metabolic/NutritionalDisorders").show(2,truncate = True))
    
    Dataset = Dataset.withColumn('ami_commodity_score',commodity_score_ami_udf(*AMI_column_list)) \
    .withColumn('ami_lace_score',lace_score_ami_udf('LengthOfStay','Inpatient_visits','ami_commodity_score','ED_visits'))
    print(Dataset.filter(Dataset.diagnosis_code.isin(AMI_diagnosis_code_list)).select("encounter_id","age","diagnosis_code","Protein-caloriemalnutrition",'LengthOfStay','Inpatient_visits','ami_commodity_score','ED_visits',"ami_lace_score").show(5,truncate = True))
    
    # calculate counts and final measure score
    total_counts = Dataset.count()
    effective_counts = Dataset.filter("ami_lace_score>9").count()
    AMI_score = float(effective_counts)/float(total_counts)
    print("total_counts = ",total_counts)
    print("effective_counts = ",effective_counts)
    print("AMI_score = ",AMI_score)
    
    '''
    header = lines.first()
    schemaString = header

    '''

	
    # Stop the session
    spark.stop()