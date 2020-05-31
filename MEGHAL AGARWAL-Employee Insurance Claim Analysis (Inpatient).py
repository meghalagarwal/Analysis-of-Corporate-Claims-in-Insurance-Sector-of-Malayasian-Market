# Packages which will be use to find the end result
import math
import xlrd
import MySQLdb
import datetime
import numpy as np
import pandas as pd
import seaborn as sns
import mysql.connector
from statistics import mode
import statsmodels.api as sm
import matplotlib.pyplot as plt
import scipy.cluster.hierarchy as sch
from sklearn import preprocessing as pp
from sklearn.cluster import KMeans as km
from scipy.cluster.hierarchy import linkage as l
from sklearn.linear_model import LogisticRegression as lr
from mlxtend.frequent_patterns import apriori, association_rules
from statsmodels.stats.outliers_influence import variance_inflation_factor as vif

# X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-

# Database Creation in MySQL
def mysql_2018(data2018):
    
    sheet1=data2018.sheet_by_name("FY2018")
    sheet1=data2018.sheet_by_index(0)
    
    # Open database connection
    db = MySQLdb.connect(host="localhost",user="Admin",password="Admin@1234")
    
    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    
    DDB="DROP DATABASE TEST"
    cursor.execute(DDB)
    
    DB="CREATE DATABASE TEST"
    cursor.execute(DB)
    
    UDB="USE TEST"
    cursor.execute(UDB)
    
    # Create Table as per requirement
    TB = "CREATE TABLE IN_18 (CLAIMS_ID VARCHAR(7),PLAN_CODE VARCHAR(10),RELATIONSHIP VARCHAR(15),ADMISSION_DATE VARCHAR(11),DISCHARGEABLE_DATE VARCHAR(11),HOSPITAL_NAME VARCHAR(100),DIAGNOSIS1 VARCHAR(150),CLAIM_TOT_BILL_AMT FLOAT,CLAIM_TOT_PAID_AMT FLOAT)"
    cursor.execute(TB)
    
    DATA_ENTRY = "INSERT INTO IN_18 (CLAIMS_ID,PLAN_CODE,RELATIONSHIP,ADMISSION_DATE,DISCHARGEABLE_DATE,HOSPITAL_NAME,DIAGNOSIS1,CLAIM_TOT_BILL_AMT,CLAIM_TOT_PAID_AMT) VALUES ('%s','%s','%s','%s','%s','%s','%s','%f','%f')"
    cursor.execute(DATA_ENTRY)
    
    for r in range(1,sheet1.nrows):
        CID=sheet1.cell(r,0).value
        PC=sheet1.cell(r,1).value
        RS=sheet1.cell(r,2).value
        AD=sheet1.cell(r,3).value
        DD=sheet1.cell(r,4).value
        HN=sheet1.cell(r,5).value
        D1=sheet1.cell(r,6).value
        CTBA=sheet1.cell(r,7).value
        CTPA=sheet1.cell(r,8).value
        
        values = (CID,PC,RS,AD,DD,HN,D1,CTBA,CTPA)
        
        cursor.execute(DATA_ENTRY,values)
    
    #for row in df.itertuples():
        #cursor.execute("INSERT INTO IN_18 (CLAIMS_ID,PLAN_CODE,RELATIONSHIP,ADMISSION_DATE,DISCHARGEABLE_DATE,HOSPITAL_NAME,DIAGNOSIS1,CLAIM_TOT_BILL_AMT,CLAIM_TOT_PAID_AMT) VALUES ('row.CLAIMS_ID','row.PLAN_CODE','row.RELATIONSHIP','row.ADMISSION_DATE','row.DISCHARGEABLE_DATE','row.HOSPITAL_NAME','row.DIAGNOSIS1','row.CLAIM_TOT_BILL_AMT','row.CLAIM_TOT_PAID_AMT')")
    
    cursor.close()
    
    db.commit()
    db.close()
    
    return

# Fecthcing the Data    
def mysql_fetch2018():
    db_connection = mysql.connector.connect(host="localhost",user="Admin",password="Admin@1234",database="TEST")
    
    my_database = db_connection.cursor()
    
    sql_statement = "SELECT 'CLAIMS_ID','PLAN_CODE','RELATIONSHIP','ADMISSION_DATE','DISCHARGEABLE_DATE','HOSPITAL_NAME','DIAGNOSIS1','CLAIM_TOT_BILL_AMT','CLAIM_TOT_PAID_AMT' FROM IN_18"
    my_database.execute(sql_statement)
    
    output = my_database.fetchall()
    
    data1 = pd.DataFrame(output, columns = ['CLAIMS_ID','PLAN_CODE','RELATIONSHIP','ADMISSION_DATE','DISCHARGEABLE_DATE','HOSPITAL_NAME','DIAGNOSIS1','CLAIM_TOT_BILL_AMT','CLAIM_TOT_PAID_AMT'])
    data1.dtypes
    
    return

# Database Creation in MySQL
def mysql_2019(data2019):
    
    sheet1=data2019.sheet_by_name("FY2019")
    sheet1=data2019.sheet_by_index(0)
    
    # Open database connection
    db = MySQLdb.connect(host="localhost",user="Admin",password="Admin@1234")
    
    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    
    DDB="DROP DATABASE TEST"
    cursor.execute(DDB)
    
    DB="CREATE DATABASE TEST"
    cursor.execute(DB)
    
    UDB="USE TEST"
    cursor.execute(UDB)
    
    # Create Table as per requirement
    TB = "CREATE TABLE IN_19 (CLAIMS_ID VARCHAR(7),PLAN_CODE VARCHAR(10),RELATIONSHIP VARCHAR(15),ADMISSION_DATE VARCHAR(11),DISCHARGEABLE_DATE VARCHAR(11),HOSPITAL_NAME VARCHAR(100),DIAGNOSIS1 VARCHAR(150),CLAIM_TOT_BILL_AMT FLOAT,CLAIM_TOT_PAID_AMT FLOAT)"
    cursor.execute(TB)
    
    DATA_ENTRY = "INSERT INTO IN_19 (CLAIMS_ID,PLAN_CODE,RELATIONSHIP,ADMISSION_DATE,DISCHARGEABLE_DATE,HOSPITAL_NAME,DIAGNOSIS1,CLAIM_TOT_BILL_AMT,CLAIM_TOT_PAID_AMT) VALUES ('%s','%s','%s','%s','%s','%s','%s','%f','%f')"
    cursor.execute(DATA_ENTRY)
    
    for r in range(1,sheet1.nrows):
        CID=sheet1.cell(r,0).value
        PC=sheet1.cell(r,1).value
        RS=sheet1.cell(r,2).value
        AD=sheet1.cell(r,3).value
        DD=sheet1.cell(r,4).value
        HN=sheet1.cell(r,5).value
        D1=sheet1.cell(r,6).value
        CTBA=sheet1.cell(r,7).value
        CTPA=sheet1.cell(r,8).value
        
        values = (CID,PC,RS,AD,DD,HN,D1,CTBA,CTPA)
        
        cursor.execute(DATA_ENTRY,values)
    
    #for row in df.itertuples():
        #cursor.execute("INSERT INTO IN_18 (CLAIMS_ID,PLAN_CODE,RELATIONSHIP,ADMISSION_DATE,DISCHARGEABLE_DATE,HOSPITAL_NAME,DIAGNOSIS1,CLAIM_TOT_BILL_AMT,CLAIM_TOT_PAID_AMT) VALUES ('row.CLAIMS_ID','row.PLAN_CODE','row.RELATIONSHIP','row.ADMISSION_DATE','row.DISCHARGEABLE_DATE','row.HOSPITAL_NAME','row.DIAGNOSIS1','row.CLAIM_TOT_BILL_AMT','row.CLAIM_TOT_PAID_AMT')")
    
    cursor.close()
    
    db.commit()
    db.close()
    
    return

# Fecthcing the Data    
def mysql_fetch2019():
    db_connection = mysql.connector.connect(host="localhost",user="Admin",password="Admin@1234",database="TEST")
    
    my_database = db_connection.cursor()
    
    sql_statement = "SELECT 'CLAIMS_ID','PLAN_CODE','RELATIONSHIP','ADMISSION_DATE','DISCHARGEABLE_DATE','HOSPITAL_NAME','DIAGNOSIS1','CLAIM_TOT_BILL_AMT','CLAIM_TOT_PAID_AMT' FROM IN_19"
    my_database.execute(sql_statement)
    
    output = my_database.fetchall()
    
    data2 = pd.DataFrame(output, columns = ['CLAIMS_ID','PLAN_CODE','RELATIONSHIP','ADMISSION_DATE','DISCHARGEABLE_DATE','HOSPITAL_NAME','DIAGNOSIS1','CLAIM_TOT_BILL_AMT','CLAIM_TOT_PAID_AMT'])
    data2.dtypes
    
    return
    
#-------------------------------------CLUSTERING-------------------------------------------------------------------------------
def clutering_2018(data1):
    clustering_data18=data1
    clustering_data19=data2
    clustering_data=pd.concat([clustering_data18,clustering_data19],axis=0)
    clustering_data.columns

    clustering_data=clustering_data.drop(['PLAN_CODE', 'RELATIONSHIP'],axis=1)
    dummy=pd.get_dummies(clustering_data[["HOSPITAL_NAME","DIAGNOSIS1"]])

    clustering_data=pd.concat([clustering_data,dummy],axis=1)
    clustering_data=clustering_data.drop(["HOSPITAL_NAME","DIAGNOSIS1"],axis=1)

    norm_data=pp.normalize(clustering_data)
    
    print("K-Means Clustering")
    kmeans_clust(norm_data) # Calling K-Means Cluster.
    
    print("Hirarical Clustering")
    Hierar_cluster1(norm_data) # Calling Hierarical Cluster of Ward Linkage.
    Hierar_cluster2(norm_data) # Calling Hierarical Cluster of Complete Linkage.
    Hierar_cluster3(norm_data) # Calling Hierarical Cluster of Average Linkage.
    
    return data1

#-------------------------------------HIERARCHICAL CLUSTERING--------------------------------------------------
# Hierarical Cluster of Ward Linkage.
def Hierar_cluster1(norm_data):
    w_clust=l(norm_data,method="ward",metric="euclidean")  # Calling Ward linkage Technique of Hierarical clustering.
    sch.dendrogram(w_clust) # Plotting Dendrogram of Ward Linkage    
    plt.title("Hierarical Clustering via WARD Technique")   # Creating a lable of the generated Dendrogram.
    
    return norm_data

# Hierarical Cluster of Complete Linkage.
def Hierar_cluster2(norm_data):
    c_clust=l(norm_data,method="complete",metric="euclidean")  # Calling Complete linkage Technique of Hierarical clustering.
    sch.dendrogram(c_clust) # Plotting Dendrogram of Complete Linkage    
    plt.title("Hierarical Clustering via COMPLETE Technique")   # Creating a lable of the generated Dendrogram.
    
    return norm_data

# Hierarical Cluster of Average Linkage.
def Hierar_cluster3(norm_data):
    a_clust=l(norm_data,method="average",metric="euclidean")   # Calling Average linkage Technique of Hierarical clustering. 
    sch.dendrogram(a_clust) # Plotting Dendrogram of Average Linkage    
    plt.title("Hierarical Clustering via AVERAGE Technique")    # Creating a lable of the generated Dendrogram.
    
    return norm_data

#-------------------------------------K-MEANS CLUSTERING--------------------------------------------------    
def kmeans_clust(norm_data):
    k=list(range(2,15)) # Creating a list of clusters ranging from 2 to 15.
    TWSS=[]
    for i in k:
        kmeans_value=km(i)  # Calculating K-Means for individual Clusters via for loop.
        kmeans_value.fit(norm_data)
        TWSS.append(kmeans_value.inertia_)  # Appending Sum of Squares of all clusters individualy 
    plt.plot(k,TWSS,"go-")  # Plotting K-Means Graph to understand the number of clusters can be considered for the given data.
    
    kmeans_val=km(3)    # After ploting above K-Means Cluster, we came to a conclusion that the number of clusters required for this data set is 4.
    kmeans_val.fit(clustering_data)
    kmeans_val.labels_
    clustering_data["Cluster_no"]=0    # Creating a Column in given dataset to segregate the data into various clusters
    new=pd.Series(kmeans_val.labels_)
    clustering_data["Cluster_no"]=new
    clustering_data.Cluster_no.value_counts()
    
    return norm_data

#-------------------------------------ASSOCIATION RULES-----------------------------------------------------
def asso_18_19(data1,data2):
    asso_18=data1[['PLAN_CODE', 'RELATIONSHIP', 'HOSPITAL_NAME', 'DIAGNOSIS1']]
    asso_19=data2[['PLAN_CODE', 'RELATIONSHIP', 'HOSPITAL_NAME', 'DIAGNOSIS1']]
    
    asso_data=pd.concat([asso_18,asso_19],axis=0)
    asso_data=pd.get_dummies(asso_data)
    
    support_value=float(input("Enter Support Value between = 0.1 to 0.0005 = "))
    quant_values=int(input("Enter Maximum numbers of items combination required between 3 to 4 = "))
    appriori(asso_data,support_value,quant_values)
    
    confi_values=float(input("Enter Minimum Threshold value of Confidance between 0 to 1 = "))
    association_rules_confi(asso_data,confi_values)
        
    association_rules_lift(asso_data)

def appriori(asso_data,support_value,quant_values):
    frequent_options=apriori(asso_data,min_support=support_value,max_len=quant_values,use_colnames=True)
    print(frequent_options.shape)
    print(frequent_options.sort_values('support',ascending = False))
    
    return (asso_data,support_value,quant_values)

def association_rules_confi(asso_data,confi_values):
    ass_rules=association_rules(frequent_options,metric="confidence",min_threshold=confi_values)
    print(ass_rules)
    plt.bar(x=list(range(1,11)),height=frequent_options.support[1:11]);plt.xticks(list(range(1,11)),frequent_options.itemsets[1:11],rotation=90)

    return asso_data,confi_values

def association_rules_lift(asso_data):    
    ass_rules1=association_rules(frequent_options,metric="lift")
    ass_rules1.sort_values("lift",ascending=False)
    plt.bar(x=list(range(1,11)),height=ass_rules1.support[1:11]);plt.xticks(list(range(1,11)),frequent_options.itemsets[1:11],rotation=90)
    print(ass_rules1)
    
    return asso_data,confi_values

#-------------------------------------RECOMMENDATION SYSTEM-----------------------------------------------------
# Recommendation Model for Hosiptals based on past claims in 2018-19
def recom_hos(data1,data2):
    data18_hos=pd.DataFrame(data1.groupby("HOSPITAL_NAME")["CLAIM_TOT_BILL_AMT"].mean())
    data19_hos=pd.DataFrame(data2.groupby("HOSPITAL_NAME")["CLAIM_TOT_BILL_AMT"].mean())
    plan_hos_bill=pd.concat([data18_hos,data19_hos],axis=0)
    plan_hos_bill.head()

    data18_plan_hos_bill=data1.groupby("HOSPITAL_NAME")["CLAIM_TOT_BILL_AMT"].count()
    data19_plan_hos_bill=data2.groupby("HOSPITAL_NAME")["CLAIM_TOT_BILL_AMT"].count()
    plan_hos_bill["Avg_Bill"]=pd.concat([data18_plan_hos_bill,data19_plan_hos_bill],axis=0)
    plan_hos_bill.head()

    plan_hos_bill.CLAIM_TOT_BILL_AMT.plot.bar()
    plan_hos_bill.Avg_Bill.plot.bar()

    sns.jointplot(x="CLAIM_TOT_BILL_AMT",y="Avg_Bill",data=plan_hos_bill)
    
    X=sm.add_constant(plan_hos_bill)
    vif_values=pd.Series([vif(X.values,i) for i in range(X.shape[1])], index=X.columns)
    print(vif_values)    

    plan_hos=data1.pivot_table(index="PLAN_CODE",values="CLAIM_TOT_BILL_AMT",columns="HOSPITAL_NAME")

    plan_hos_bill.sort_values("Avg_Bill",ascending=False)
    
    Hos_name=plan_hos["Hospital Melaka"]
    
    hos=plan_hos.corrwith(Hos_name)
    
    Corr_Hosp = pd.DataFrame(hos, columns=['Correlation'])
    Corr_Hosp.dropna(inplace=True)
    print(Corr_Hosp)    

# Recommendation Model for Possible Diagnosis based on past claims in 2018-19
def recom_diag(data1,data2):
    data18_diag=pd.DataFrame(data1.groupby("DIAGNOSIS1")["CLAIM_TOT_BILL_AMT"].mean())
    data19_diag=pd.DataFrame(data2.groupby("DIAGNOSIS1")["CLAIM_TOT_BILL_AMT"].mean())
    plan_dia_bill=pd.concat([data18_diag,data19_diag],axis=0)
    plan_dia_bill.head()

    data18_plan_dia_bill=data1.groupby("DIAGNOSIS1")["CLAIM_TOT_BILL_AMT"].count()
    data19_plan_dia_bill=data2.groupby("DIAGNOSIS1")["CLAIM_TOT_BILL_AMT"].count()
    plan_dia_bill["Avg_Bill"]=pd.concat([data18_plan_dia_bill,data19_plan_dia_bill],axis=0)
    plan_dia_bill.head()
    
    plan_dia_bill.CLAIM_TOT_BILL_AMT.plot.bar()
    plan_dia_bill.Avg_Bill.plot.bar()

    sns.jointplot(x="CLAIM_TOT_BILL_AMT",y="Avg_Bill",data=plan_hos_bill)
    
    X=sm.add_constant(plan_dia_bill)
    vif_values=pd.Series([vif(X.values,i) for i in range(X.shape[1])], index=X.columns)
    print(vif_values)    

    plan_dia=data1.pivot_table(index="PLAN_CODE",values="CLAIM_TOT_BILL_AMT",columns="DIAGNOSIS1")

    plan_dia_bill.sort_values("Avg_Bill",ascending=False)
    
    Dis_name=plan_dia["N63 , Unspecified lump in breast"]
    
    dis=plan_dia.corrwith(Dis_name)
    
    Corr_Dis = pd.DataFrame(dis, columns=['Correlation'])
    Corr_Dis.dropna(inplace=True)
    print(Corr_Dis)        
 
#-------------------------------------EDA-------------------------------------------------------------------------------------------------------------------------------------
# EDA on FY2018 Sheet
def eda_2018 (data1):
    data1.head()
    
    data1["CLAIM_TOT_BILL_AMT"]=data1["CLAIM_TOT_BILL_AMT"].astype(float)
    data1["CLAIM_TOT_PAID_AMT"]=data1["CLAIM_TOT_PAID_AMT"].astype(float)
    data1["ADMISSION_DATE"]=pd.to_datetime(data1["ADMISSION_DATE"])
    data1["DISCHARGEABLE_DATE"]=pd.to_datetime(data1["DISCHARGEABLE_DATE"])
    data1["AD_Date"]=pd.DatetimeIndex(data1["ADMISSION_DATE"]).day
    data1["AD_Month"]=pd.DatetimeIndex(data1["ADMISSION_DATE"]).month
    data1["DI_Date"]=pd.DatetimeIndex(data1["DISCHARGEABLE_DATE"]).day
    data1["DI_Month"]=pd.DatetimeIndex(data1["DISCHARGEABLE_DATE"]).month
    data1["NO_OF_DAYS"]=data1["DI_Month"]-data1["AD_Month"]
    data1=data1[data1.NO_OF_DAYS>=0]
    data1["NO_OF_DAYS"]=abs(data1["DI_Date"]-data1["AD_Date"])
    data1["NO_OF_MONTHS"]=abs(data1["DI_Month"]-data1["AD_Month"])
    data1["DAYS_IN_HOS"]=abs((30*data1["NO_OF_MONTHS"])-data1["NO_OF_DAYS"])
    duplicate_rows_df_2k19 = data1[data1.duplicated()]
    data1=data1.drop_duplicates()
    data1=data1.drop(["CLAIMS_ID","NO_OF_DAYS","NO_OF_MONTHS","AD_Date","DI_Date","DI_Month","CLAIM_TOT_PAID_AMT","ADMISSION_DATE","DISCHARGEABLE_DATE","DAYS_IN_HOS"],axis=1)
     
    data1.describe()

    data1.PLAN_CODE.unique()
    data1.PLAN_CODE.value_counts()
    data1.PLAN_CODE=data1.PLAN_CODE.map({"PLAN 1":1,"PLAN 2":2,"PLAN 3":3})

    data1.RELATIONSHIP.unique()
    data1.RELATIONSHIP.value_counts()
    data1.RELATIONSHIP=data1.RELATIONSHIP.map({"PRINCIPAL":1,"HUSBAND/WIFE":2,"CHILDREN":3})
    
    data1.HOSPITAL_NAME.value_counts()
    data1.DIAGNOSIS1.value_counts()    
    data1.AD_Month.value_counts()
        
    data1.PLAN_CODE.plot.kde()
    data1.RELATIONSHIP.plot.kde()
    data1.CLAIM_TOT_BILL_AMT.plot.kde()
    data1.AD_Month.plot.kde(ind=[1,2,3,4,5,6,7,8,9,10,11,12])
    
    plt.boxplot(data1.PLAN_CODE)    
    plt.boxplot(data1.RELATIONSHIP)
    plt.boxplot(data1.CLAIM_TOT_BILL_AMT)
    
    sns.countplot(data1.PLAN_CODE)
    sns.countplot(data1.AD_Month)
    sns.countplot(data1.RELATIONSHIP)
    sns.distplot(data1.CLAIM_TOT_BILL_AMT)
    
    sns.heatmap(data1.corr(),vmin=0,vmax=1,annot=True,linewidths=1,linecolor="Red")

    sns.pairplot(data1,diag_kind="kde")
    
    data1.groupby("PLAN_CODE")["CLAIM_TOT_BILL_AMT"].sum().plot.bar().grid(True)
    data10=data1.groupby("PLAN_CODE")["CLAIM_TOT_BILL_AMT"].sum();print(data10)
    plt.pie(data10,explode=(0.1, 0.1, 0.1),autopct='%1.1f%%',radius=1.5,labels=("Plan 1","Plan 2","Plan 3"))    
    data1.groupby("PLAN_CODE")["HOSPITAL_NAME"].count().plot.bar()
    data1.groupby("PLAN_CODE")["DIAGNOSIS1"].count().plot.bar()
    
    data1.groupby("RELATIONSHIP")["PLAN_CODE"].count().plot.bar().grid()
    data11=data1.groupby("RELATIONSHIP")["PLAN_CODE"].count();print(data11)
    plt.pie(data11,explode=(0.1, 0.1, 0.1),autopct='%1.1f%%',radius=1.5,labels=("Employees","Spouse","Children"))
    
    data1.groupby("RELATIONSHIP")["CLAIM_TOT_BILL_AMT"].sum().plot.bar().grid()
    data12=data1.groupby("RELATIONSHIP")["CLAIM_TOT_BILL_AMT"].sum();print(data12)
    plt.pie(data12,explode=(0.1, 0.1, 0.1),autopct='%1.1f%%',radius=1.5,labels=("Employees","Spouse","Children"))
    
    data13=data1.groupby(["RELATIONSHIP","PLAN_CODE"])["CLAIM_TOT_BILL_AMT"].sum();print(data13)
    plt.pie(data13,explode=(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1),autopct='%1.1f%%',radius=1.5,labels=("Employees+Plan 1","Employees+Plan 2","Employees+Plan 3","Spouse+Plan 1","Spouse+Plan 2","Spouse+Plan 3","Children+Plan 1","Children+Plan 2","Children+Plan 3"))
    
    data14=data1.groupby(["PLAN_CODE","RELATIONSHIP"])["CLAIM_TOT_BILL_AMT"].sum();print(data14)
    data15=data1.groupby(["HOSPITAL_NAME"])["CLAIM_TOT_BILL_AMT"].sum();print(data15)
    
    data16=data1.groupby(["DIAGNOSIS1"])["CLAIM_TOT_BILL_AMT"].sum().plot.bar().grid()
    
    data17=data2.groupby(["HOSPITAL_NAME"])["DIAGNOSIS1"].count().plot.bar().grid()
    
    data18=data1.groupby("PLAN_CODE")["HOSPITAL_NAME"].count();print(data18)
    
    plt.pie(data10,explode=(0.1, 0.1, 0.1),autopct='%1.1f%%',radius=1.5)
    
    return data1
    
# EDA on FY2019 Sheet
def eda_2019 (data2):
    data2.head()
    
    data2["CLAIM_TOT_BILL_AMT"]=data2["CLAIM_TOT_BILL_AMT"].astype(float)
    data2["CLAIM_TOT_PAID_AMT"]=data2["CLAIM_TOT_PAID_AMT"].astype(float)
    data2["ADMISSION_DATE"]=pd.to_datetime(data2["ADMISSION_DATE"])    
    data2["DISCHARGEABLE_DATE"]=pd.to_datetime(data2["DISCHARGEABLE_DATE"])
    data2["AD_Date"]=pd.DatetimeIndex(data2["ADMISSION_DATE"]).day
    data2["AD_Month"]=pd.DatetimeIndex(data2["ADMISSION_DATE"]).month
    data2["DI_Date"]=pd.DatetimeIndex(data2["DISCHARGEABLE_DATE"]).day
    data2["DI_Month"]=pd.DatetimeIndex(data2["DISCHARGEABLE_DATE"]).month
    data2["NO_OF_DAYS"]=data2["DI_Month"]-data2["AD_Month"]
    data2=data2[data2.NO_OF_DAYS>=0]
    data2["NO_OF_DAYS"]=abs(data2["DI_Date"]-data2["AD_Date"])
    data2["NO_OF_MONTHS"]=abs(data2["DI_Month"]-data2["AD_Month"])
    data2["DAYS_IN_HOS"]=abs((30*data2["NO_OF_MONTHS"])-data2["NO_OF_DAYS"])
    duplicate_rows_df_2k18 = data2[data2.duplicated()]
    data2=data2.drop_duplicates()
    data2=data2.drop(["CLAIMS_ID","NO_OF_DAYS","NO_OF_MONTHS","AD_Date","DI_Date","DI_Month","CLAIM_TOT_PAID_AMT","ADMISSION_DATE","DISCHARGEABLE_DATE","DAYS_IN_HOS"],axis=1)
    
    data2.describe()

    data2.PLAN_CODE.unique()
    data2.PLAN_CODE.value_counts()
    data2.PLAN_CODE=data2.PLAN_CODE.map({"PLAN 1":1,"PLAN 2":2,"PLAN 3":3})

    data2.RELATIONSHIP.unique()
    data2.RELATIONSHIP.value_counts()
    data2.RELATIONSHIP=data2.RELATIONSHIP.map({"PRINCIPAL":1,"HUSBAND/WIFE":2,"CHILDREN":3})
    
    data2.HOSPITAL_NAME.value_counts()    
    data2.DIAGNOSIS1.value_counts()    
    data2.AD_Month.value_counts()
        
    data2.PLAN_CODE.plot.kde()
    data2.RELATIONSHIP.plot.kde()
    data2.CLAIM_TOT_BILL_AMT.plot.kde()
    data2.AD_Month.plot.kde(ind=[1,2,3,4,5,6,7,8,9,10,11,12])
    
    plt.boxplot(data2.PLAN_CODE)    
    plt.boxplot(data2.RELATIONSHIP)
    plt.boxplot(data2.CLAIM_TOT_BILL_AMT)
    
    sns.countplot(data2.PLAN_CODE)
    sns.countplot(data2.AD_Month)
    sns.countplot(data2.RELATIONSHIP)
    sns.distplot(data2.CLAIM_TOT_BILL_AMT)
    
    sns.heatmap(data2.corr(),vmin=0,vmax=1,annot=True,linewidths=1,linecolor="Red")

    sns.pairplot(data2,diag_kind="kde")
    
    data2.groupby("PLAN_CODE")["CLAIM_TOT_BILL_AMT"].sum().plot.bar().grid()
    data20=data2.groupby("PLAN_CODE")["CLAIM_TOT_BILL_AMT"].sum();print(data20)
    plt.pie(data20,explode=(0.1, 0.1, 0.1),autopct='%1.1f%%',radius=1.5,labels=("Plan 1","Plan 2","Plan 3"))    
    data2.groupby("PLAN_CODE")["HOSPITAL_NAME"].count().plot.bar()
    data2.groupby("PLAN_CODE")["DIAGNOSIS1"].count().plot.bar()
    
    data2.groupby("RELATIONSHIP")["PLAN_CODE"].count().plot.bar().grid()
    data21=data2.groupby("RELATIONSHIP")["PLAN_CODE"].count();print(data21)
    plt.pie(data21,explode=(0.1, 0.1, 0.1),autopct='%1.1f%%',radius=1.5,labels=("Employees","Spouse","Children"))
    
    data2.groupby(["RELATIONSHIP","PLAN_CODE"]).count()
    
    data2.groupby("RELATIONSHIP")["CLAIM_TOT_BILL_AMT"].sum().plot.bar().grid()
    data22=data2.groupby("RELATIONSHIP")["CLAIM_TOT_BILL_AMT"].sum();print(data22)
    plt.pie(data22,explode=(0.1, 0.1, 0.1),autopct='%1.1f%%',radius=1.5,labels=("Employees","Spouse","Children"))
    
    data23=data2.groupby(["RELATIONSHIP","PLAN_CODE"])["CLAIM_TOT_BILL_AMT"].sum();print(data23)
    plt.pie(data23,explode=(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1),autopct='%1.1f%%',radius=1.5,labels=("Employees+Plan 1","Employees+Plan 2","Employees+Plan 3","Spouse+Plan 1","Spouse+Plan 2","Spouse+Plan 3","Children+Plan 1","Children+Plan 2","Children+Plan 3"))
   
    data25=data2.groupby(["HOSPITAL_NAME"])["DIAGNOSIS1"].count().plot.bar().grid()
   
    data26=data2.groupby(["DIAGNOSIS1"])["CLAIM_TOT_BILL_AMT"].sum();print(data26)
    plt.pie(data26,autopct='%1.1f%%')
    
    data27=data2.groupby("PLAN_CODE")["HOSPITAL_NAME"].count();print(data27)
    
    plt.pie(data20,explode=(0.1, 0.1, 0.1),autopct='%1.1f%%',radius=1.5)
    
    return data2

#-------------------------------------MAIN FUNCTION-----------------------------------------------------------------------------------------
def main():    
    data2018=xlrd.open_workbook("C:/Users/hp/Dropbox/ExcelR/Projects/Employee Insurance Claim Analysis/Sample Data/Refined_MASTER_DATA_INPATIENT.xlsx")    
    mysql_2018(data2018)
    mysql_fetch2018
    
    data2019=xlrd.open_workbook("C:/Users/hp/Dropbox/ExcelR/Projects/Employee Insurance Claim Analysis/Sample Data/Refined_MASTER_DATA_INPATIENT.xlsx")
    mysql_2019(data2019)
    mysql_fetch2019
    
    # Getting Excel File from the Path where it is stored and storing FY2018 Sheet in data1.
    data1=pd.read_excel("C:/Users/hp/Dropbox/ExcelR/Projects/Employee Insurance Claim Analysis/Sample Data/Refined_MASTER_DATA_INPATIENT.xlsx","FY2018")
    # Getting Excel File from the Path where it is stored and storing FY2019 Sheet in data2.
    data2=pd.read_excel("C:/Users/hp/Dropbox/ExcelR/Projects/Employee Insurance Claim Analysis/Sample Data/Refined_MASTER_DATA_INPATIENT.xlsx","FY2019")

    # Clustering to Find the Distribution is Correct or not.
    clutering_2018(data1)
    
    # Association Rules on Plan Code, Hospitals, Relations & Diagnosis
    asso_18_19(data1,data2)

    # Calling EDA Function for 2018 Data
    eda_2018(data1)    
    # Calling EDA Function for 2019 Data
    eda_2019(data2)
    
    # Uploading Dataset into MySQL
    #mysql_2018(data)
    
    # Recommendation of Hospitals on the basis of Bill Amounts in each Plan Code
    recom_hos(data1,data2)    
    # Recommendation of Diagnosis on the basis of Bill Amounts in each Plan Code
    recom_diag(data1,data2)    
    
# X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-X-

# Executing Programe by calling Main Function
main()