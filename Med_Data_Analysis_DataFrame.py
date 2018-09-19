from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import os
import sys


AMI_diagnosis_code_list=['410.00', '410.01', '410.10', '410.11', '410.20', '410.21', '410.30', '410.31', '410.40', '410.41', '410.50', \
'410.51', '410.60', '410.61', '410.70', '410.71', '410.80', '410.81', '410.90', '410.91']

AMI_column_list = ('HistoryofPTCA', 'HistoryofCABG', 'Congestiveheartfailure', 'Acutecoronarysyndrome', 'Anteriormyocardialinfarction', 'Otherlocationofmyocardialinfarction', 
'Anginapectorisoldmyocardialinfarction', 'Coronaryatherosclerosis', 'Valvularorrheumaticheartdisease', 'Specifiedarrhythmias', 'Historyofinfection', 
'Metastaticcanceroracuteleukemia', 'Cancer', 'Diabetesmellitus(DM)orDMcomplications', 'Protein-caloriemalnutrition', 'Disordersoffluidelectrolyteacid-base', 
'Irondeficiencyorotheranemiasandblooddisease', 'Dementiaorotherspecifiedbraindisorders', 'Hemiplegiaparaplegiaparalysisfunctionaldisability', 'Stroke', 'Cerebrovasculardisease',
'Vascularorcirculatorydisease', 'Chronicobstructivepulmonarydisease', 'Asthma', 'Pneumonia', 'End-stagerenaldiseaseordialysis', 'Renalfailure', 
'Otherurinarytractdisorders', 'Decubitusulcerorchronicskinulcer' )

COPD_diagnosis_code_list=['491.21', '491.22', '491.8', '491.9', '492.8', '493.20', '493.21', '493.22', '496', '518.81', '518.82', '518.84', '799.1']
COPD_column_list = ('HistoryofMechanicalVentilation', 'SleepApnea', 'Respiratordependence/tracheostomystatus', 'Cardio-respiratoryfailureorcardio-respiratoryshock', 
'Congestiveheartfailure', 'Acutecoronarysyndrome', 'Coronaryatherosclerosisoranginacerebrovasculardisease', 'Specifiedarrhythmias', 'OtherandUnspecifiedHeartDisease', 
'Vascularorcirculatorydisease', 'Fibrosisoflungandotherchroniclungdisorders', 'Pneumonia', 'Historyofinfection', 'Metastaticcanceroracuteleukemia', 
'LungUpperDigestiveTractandOtherSevereCancers', 'LymphaticHeadandNeckBrainandOtherMajorCancers;BreastColorectalandotherCancersandTumors;OtherRespiratoryandHeartNeoplasms', 
'OtherDigestiveandUrinaryNeoplasms', 'Diabetesmellitus(DM)orDMcomplications', 'Protein-caloriemalnutrition', 'Disordersoffluidelectrolyteacid-base', 
'OtherEndocrine/Metabolic/NutritionalDisorders', 'PancreaticDisease', 'PepticUlcerHemorrhageOtherSpecifiedGastrointestinalDisorders', 'OtherGastrointestinalDisorders', 
'SevereHematologicalDisorders', 'Irondeficiencyorotheranemiasandblooddisease', 'Dementiaorotherspecifiedbraindisorders', 'Drug/AlcoholInducedDependence/Psychosis', 
'MajorPsychiatricDisorders', 'Depression', 'AnxietyDisorders', 'OtherPsychiatricDisorders', 'QuadriplegiaParaplegiaParalysisFunctionalDisability', 'Polyneuropathy', 
'HypertensiveHeartandRenalDiseaseorEncephalopathy')


HF_diagnosis_code_list=['402.01', '402.11', '402.91', '404.01', '404.03', '404.11', '404.13', '404.91', '404.93', '428.xx']
HF_column_list = ('HistoryofCABG', 'Septicemia/shock', 'Congestiveheartfailure', 'Acutecoronarysyndrome', 'Coronaryatherosclerosisoranginacerebrovasculardisease', 
'Valvularorrheumaticheartdisease', 'Specifiedarrhythmias', 'Vascularorcirculatorydisease', 'OtherandUnspecifiedHeartDisease', 'Metastaticcanceroracuteleukemia', 'Cancer', 
'Diabetesmellitus(DM)orDMcomplications', 'Protein-caloriemalnutrition', 'Disordersoffluidelectrolyteacid-base', 'Liverandbiliarydisease', 
'PepticUlcerHemorrhageOtherSpecifiedGastrointestinalDisorders', 'OtherGastrointestinalDisorders', 'SevereHematologicalDisorders', 'Irondeficiencyorotheranemiasandblooddisease',
'Dementiaorotherspecifiedbraindisorders', 'Drug/AlcoholInducedDependence/Psychosis', 'MajorPsychiatricDisorders', 'Depression', 'OtherPsychiatricDisorders', 
'Hemiplegiaparaplegiaparalysisfunctionaldisability', 'Stroke', 'Chronicobstructivepulmonarydisease', 'Fibrosisoflungandotherchroniclungdisorders', 'Asthma', 'Pneumonia', 
'End-stagerenaldiseaseordialysis', 'Renalfailure', 'Nephritis', 'Otherurinarytractdisorders', 'Decubitusulcerorchronicskinulcer')


HWR_diagnosis_code_list=['CCS']
HWR_column_list = ('Historyofinfection', 'Pneumonia', 'Metastaticcanceroracuteleukemia', 'LungUpperDigestiveTractandOtherSevereCancers', 
'LymphaticHeadandNeckBrainandOtherMajorCancers;BreastColorectalandotherCancersandTumors;OtherRespiratoryandHeartNeoplasms', 'Diabetesmellitus(DM)orDMcomplications', 
'Protein-caloriemalnutrition', 'End-stageliverdisease', 'Coagulationdefectsandotherspecifiedhematologicaldisorders', 'Drug/AlcoholInducedDependence/Psychosis', 
'MajorPsychiatricDisorders','Hemiplegiaparaplegiaparalysisfunctionaldisability', 'Seizuredisordersandconvulsions', 'Chronicheartfailure', 
'Coronaryatherosclerosisoranginacerebrovasculardisease', 'Specifiedarrhythmias', 'Chronicobstructivepulmonarydisease', 'Fibrosisoflungandotherchroniclungdisorders', 
'Dialysisstatus', 'Decubitusulcerorchronicskinulcer', 'Septicemia/shock', 'Disordersoffluidelectrolyteacid-base', 'Irondeficiencyorotheranemiasandblooddisease', 'Renalfailure', 
'PancreaticDisease', 'Rheumatoidarthritisandinflammatoryconnectivetissuedisease', 'Respiratordependence/tracheostomystatus', 'Transplants', 
'Coagulationdefectsandotherspecifiedhematologicaldisorders', 'Hipfracture/dislocation')


# get the commodity_score for a certain measure
# based on counts of 'Yes' over a list of columns
# input: args = () -- list_of_columns for a certain measure
# example input: args = ('HistoryofPTCA','HistoryofCABG')
def commodity_score_fun(*args):
    value = 0
    for arg in args:
        if(arg == 'Yes'):
            value += 1
    return value   
    
    
# lace score of a certain measure at a certain row
# (LengthOfStay,Inpatient_visits,Commodity_Score, EDVisit)
# 
def lace_score_fun(LengthOfStay,Inpatient_visits,Commodity_Score,ED_visits): 
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
    if(Inpatient_visits == 'Yes'):
        value += 3

    # Add points due to Commodity_Score 
    Commodity_Score_int = int(Commodity_Score)
    if(Commodity_Score_int>=4):
        value += 5
    else:
        value += Commodity_Score_int

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

def main(arg):
    # Create a SparkSession
    print('Start Data Analysis')
    spark = SparkSession.builder.appName("Sample Data Analysis").getOrCreate()

    # Get the raw data
    
    Dataset = spark.read.option("header","true").csv("hdfs:///user/maria_dev/SampleDataAnalysis/Sample_med_data_2016.csv") # DataFrame
    

    # register into pyspark udf
    # an example of udf
    # fun1_udf = udf(lambda z: fun1(z), StringType()) 
    commodity_score_udf = udf(commodity_score_fun, IntegerType())
    # lace_score_ami_fun(LengthOfStay,EmergencyAdmission,Commodity_Score,EDVisit)
    lace_score_udf = udf(lace_score_fun, IntegerType())
    
    
    print('data loaded')
    # print(Dataset.printSchema())
    # print(Dataset.show(2,truncate = True))

    if(arg == 'AMI'):
        Dataset = Dataset.withColumn('ami_commodity_score',commodity_score_udf(*AMI_column_list)) \
        .withColumn('ami_lace_score',lace_score_udf('LengthOfStay','Inpatient_visits','ami_commodity_score','ED_visits'))
        print(Dataset.filter(Dataset.diagnosis_code.isin(AMI_diagnosis_code_list)).select('encounter_id','race','gender','age','diagnosis_code','LengthOfStay','Inpatient_visits','ami_commodity_score','ED_visits','ami_lace_score').show(5,truncate = True))
    
        # calculate counts and final measure score
        total_counts = Dataset.count()
        effective_counts = Dataset.filter("ami_lace_score>9").count()
        AMI_score = float(effective_counts)/float(total_counts)
        print("total_counts: ",total_counts)
        print("effective_counts: ",effective_counts)
        print("AMI_score: ",AMI_score)
        
    elif(arg == 'COPD'):
        Dataset = Dataset.withColumn('copd_commodity_score',commodity_score_udf(*COPD_column_list)) \
        .withColumn('copd_lace_score',lace_score_udf('LengthOfStay','Inpatient_visits','copd_commodity_score','ED_visits'))
        print(Dataset.filter(Dataset.diagnosis_code.isin(COPD_diagnosis_code_list)).select('encounter_id','race','gender','age','diagnosis_code','LengthOfStay','Inpatient_visits','copd_commodity_score','ED_visits','copd_lace_score').show(5,truncate = True))

        # calculate counts and final measure score
        total_counts = Dataset.count()
        effective_counts = Dataset.filter("copd_lace_score>9").count()
        COPD_score = float(effective_counts)/float(total_counts)
        print("total_counts: ",total_counts)
        print("effective_counts: ",effective_counts)
        print("COPD_score: ",COPD_score)        

    elif(arg == 'HF'):
        Dataset = Dataset.withColumn('hf_commodity_score',commodity_score_udf(*HF_column_list)) \
        .withColumn('hf_lace_score',lace_score_udf('LengthOfStay','Inpatient_visits','hf_commodity_score','ED_visits'))
        print(Dataset.filter(Dataset.diagnosis_code.isin(HF_diagnosis_code_list)).select('encounter_id','race','gender','age','diagnosis_code','LengthOfStay','Inpatient_visits','hf_commodity_score','ED_visits','hf_lace_score').show(5,truncate = True))

        # calculate counts and final measure score
        total_counts = Dataset.count()
        effective_counts = Dataset.filter("hf_lace_score>9").count()
        HF_score = float(effective_counts)/float(total_counts)
        print("total_counts: ",total_counts)
        print("effective_counts: ",effective_counts)
        print("HF_score: ",HF_score)               
        
    elif(arg == None):
        print('Not an effective measure name.')
    

    # Stop the session
    spark.stop()
    

if __name__ == "__main__":

    main(sys.argv[1]) 