import pandas as pd
import datetime
from datetime import timedelta

path_db_masters = r'E:\001_CMG\HOME\Reporting Culture\_ db MASTERs.csv'
path_db_CEs =  r'E:\001_CMG\HOME\Reporting Culture\_ db CEs.csv'
path_db_incidents =  r'E:\001_CMG\HOME\Reporting Culture\_ db CMG_Incidents.xlsx'

path_newIncidentsPerDayPerShip = r'E:\003_Python_CMG\008_Reporting_Culture\Reporting-Culture\db_newIncidentsPerDay_PerShip.csv'
path_newIncidentsPerDayPerMaster = r'E:\003_Python_CMG\008_Reporting_Culture\Reporting-Culture\db_newIncidentsPerDay_PerMaster.csv'
path_newIncidentsPerDayPerCE = r'E:\003_Python_CMG\008_Reporting_Culture\Reporting-Culture\db_newIncidentsPerDay_PerCE.csv'

path_nameOfMasterForEachShipPerDay = r'E:\003_Python_CMG\008_Reporting_Culture\Reporting-Culture\df_activeMasterDayByDayByName.csv'
path_nameOfCEForEachShipPerDay = r'E:\003_Python_CMG\008_Reporting_Culture\Reporting-Culture\df_activeCEDayByDayByName.csv'

flag_MasterCE_name = "Crew Member"
# Contract Position;
# Role Position;
# Functional Position;
flag_MasterCE_ship = "Ship"
flag_MasterCE_embarkDate = "Embark Date"
flag_MasterCE_disembarkDate = "Disembark Date"
flag_MasterCE_daysOnboard = "Embark Days"

flag_incidents_Brand = "Operating Line"
flag_incidents_date = "Event Date"
flag_incidents_Ship = "SHIP"

flag_result_date = "Date"

startDate = datetime.datetime(2017, 1, 1, 0, 0, 0)

testDate = datetime.datetime(2017, 4, 17, 0, 0, 0)

' #####################################################################################################################'
def func_readSourceData(
    df_masters, df_ces, df_incidents
):
    df_masters =  pd.read_csv(path_db_masters, sep = ";")
    df_ces = pd.read_csv(path_db_CEs, sep=";")

    # df_masters[flag_MasterCE_embarkDate] = df_masters[flag_MasterCE_embarkDate]

    xlsFileHandle = pd.ExcelFile(path_db_incidents)

    df_incidents = pd.read_excel(
        xlsFileHandle,
        sheet_name="aida_costa_incidents",
        dtype=str,
        skiprows=0
    )

    print("incident lines before brand filter " + str(df_incidents.shape[0]))
    df_incidents = df_incidents[
        (df_incidents[flag_incidents_Brand] == "COSTA EUROPE") |
        (df_incidents[flag_incidents_Brand] == "AIDA") |
        (df_incidents[flag_incidents_Brand] == "COSTA ASIA")
    ]
    print("incident lines after brand filter " + str(df_incidents.shape[0]))

    df_incidents = df_incidents.reset_index(drop=True)

    #region harmonize ship names
    df_masters[flag_MasterCE_ship] = df_masters[flag_MasterCE_ship].str.upper()
    df_ces[flag_MasterCE_ship] = df_ces[flag_MasterCE_ship].str.upper()

    df_incidents[flag_incidents_Ship] = df_incidents[flag_incidents_Ship].str.upper()
    for ap in df_incidents.index:
        if df_incidents.loc[ap, flag_incidents_Ship][:5] == "COSTA":
            df_incidents.loc[ap, flag_incidents_Ship] = df_incidents.loc[ap, flag_incidents_Ship][6:]
            # print("changed the costa ship to " + df_incidents.loc[ap, flag_incidents_Ship])
    #endregion

    #region filter out Company Activity
    print("df_masters files lines before filer out company acitivity " + str(df_masters.shape[0]))
    print("df_ces files lines before filer out company acitivity " + str(df_ces.shape[0]))
    df_masters = df_masters[df_masters[flag_MasterCE_ship] != "Company Activity".upper()]
    df_ces = df_ces[df_ces[flag_MasterCE_ship] != "Company Activity".upper()]

    df_masters = df_masters.reset_index(drop=True)
    df_ces = df_ces.reset_index(drop=True)
    print("df_masters files lines after filer out company acitivity " + str(df_masters.shape[0]))
    print("df_ces files lines after filer out company acitivity " + str(df_ces.shape[0]))
    #endregion

    #region convert time stamps
    df_masters[flag_MasterCE_embarkDate] = pd.to_datetime(df_masters[flag_MasterCE_embarkDate], format='%d.%m.%Y')#.strftime('%Y.%m.%d')#.astype('datetime64[ns]')
    df_masters[flag_MasterCE_disembarkDate] = pd.to_datetime(df_masters[flag_MasterCE_disembarkDate], format='%d.%m.%Y')#.strftime('%Y.%m.%d')#.astype('datetime64[ns]')

    df_ces[flag_MasterCE_embarkDate] = pd.to_datetime(df_ces[flag_MasterCE_embarkDate], format='%d.%m.%Y')#.strftime('%Y.%m.%d')#.astype('datetime64[ns]')
    df_ces[flag_MasterCE_disembarkDate] = pd.to_datetime(df_ces[flag_MasterCE_disembarkDate], format='%d.%m.%Y')#.strftime('%Y.%m.%d')#.astype('datetime64[ns]')

    df_incidents[flag_incidents_date] = pd.to_datetime(df_incidents[flag_incidents_date], format='%Y-%m-%d')#.strftime('%Y.%m.%d')#.astype('datetime64[ns]')
    #endregion

    # thisMaster = df_masters[
    #     (testDate > df_masters[flag_MasterCE_embarkDate]) &
    #     (testDate <= df_masters[flag_MasterCE_disembarkDate]) &
    #     (df_masters[flag_MasterCE_ship] == "SERENA")
    #     ].iloc[0][flag_MasterCE_name]
    #
    # df_masters.to_csv("df_masters_TEST.csv", sep=";")
    # #
    # subDF = df_masters[
    #     (testDate > df_masters[flag_MasterCE_embarkDate]) &
    #     (testDate <= df_masters[flag_MasterCE_disembarkDate]) &
    #     (df_masters[flag_MasterCE_ship] == "SERENA")
    #     ]

    # print("len subdf" + str(subDF.shape[0]))

    # print("thisMaster: " + thisMaster)

    print(df_masters.head(5))
    print(df_ces.head(5))
    print(df_incidents.head(5))

    return df_masters, df_ces, df_incidents

' #####################################################################################################################'
def func_loopIncidents(
    df_masters,
    df_ces,
    df_incidents,
    df_newCasesPerDay_ByShip,
    df_newCasesPerDay_ByCpt,
    df_newCasesPerDay_ByCE,
    df_activeMasterDayByDayByName, df_activeCEDayByDayByName,
    df_uptrend_Masters
):
    sr = pd.Series(pd.date_range(startDate, periods=1735, freq='D'))

    #region prepare daily structure
    for thisDay in sr.index:
        if thisDay < sr.index.max():
            thisDate = (startDate + timedelta(days=thisDay)).strftime('%Y-%m-%d')
            # print("thisDay: " + str(thisDate))

            df_newCasesPerDay_ByShip = df_newCasesPerDay_ByShip.append(
                {
                    flag_result_date: thisDate
                }, ignore_index=True
            )

            df_newCasesPerDay_ByCpt = df_newCasesPerDay_ByCpt.append(
                {
                    flag_result_date: thisDate
                }, ignore_index=True
            )

            df_newCasesPerDay_ByCE = df_newCasesPerDay_ByCE.append(
                {
                    flag_result_date: thisDate
                }, ignore_index=True
            )

            df_activeMasterDayByDayByName = df_activeMasterDayByDayByName.append(
                {
                    flag_result_date: thisDate
                }, ignore_index=True
            )

            df_activeCEDayByDayByName = df_activeCEDayByDayByName.append(
                {
                    flag_result_date: thisDate
                }, ignore_index=True
            )
    #endregion

    df_newCasesPerDay_ByShip = df_newCasesPerDay_ByShip.reset_index(drop=True)
    for thisShip in df_incidents[flag_incidents_Ship].unique():
        df_newCasesPerDay_ByShip[thisShip] = 0
        df_activeMasterDayByDayByName[thisShip] = ""
        df_activeCEDayByDayByName[thisShip] = ""

    print(df_newCasesPerDay_ByShip.head(5))

    df_newCasesPerDay_ByCpt = df_newCasesPerDay_ByCpt.reset_index(drop=True)
    for thisCPT in df_masters[flag_MasterCE_name].unique():
        df_newCasesPerDay_ByCpt[thisCPT] = 0

    print(df_newCasesPerDay_ByCpt.head(5))

    df_newCasesPerDay_ByCE = df_newCasesPerDay_ByCE.reset_index(drop=True)
    for thisCE in df_ces[flag_MasterCE_name].unique():
        df_newCasesPerDay_ByCE[thisCE] = 0

    print(df_newCasesPerDay_ByCE.head(5))

    refreshAmountOfIncidentsPerDay = False
    logDetails = False
    #region create dataframe with new cases per day by MASTER CE SHIP
    if refreshAmountOfIncidentsPerDay:
        lines = df_incidents.index.max()
        for ap in df_incidents.index:
            incidentDate = df_incidents.loc[ap, flag_incidents_date].strftime('%Y-%m-%d')
            incidentShip = df_incidents.loc[ap, flag_incidents_Ship]
            print("incident @ " + str(incidentDate) + " on board " + incidentShip)

            try:
                thisMaster = df_masters[
                    (incidentDate > df_masters[flag_MasterCE_embarkDate]) &
                    (incidentDate <= df_masters[flag_MasterCE_disembarkDate]) &
                    (df_masters[flag_MasterCE_ship] == incidentShip)
                ].iloc[0][flag_MasterCE_name]
            except:
                if logDetails:
                    print("no master found ")

            try:
                thisCE = df_ces[
                    (incidentDate > df_ces[flag_MasterCE_embarkDate]) &
                    (incidentDate <= df_ces[flag_MasterCE_disembarkDate]) &
                    (df_ces[flag_MasterCE_ship] == incidentShip)
                    ].iloc[0][flag_MasterCE_name]
            except:
                if logDetails:
                    print("no ce found ")

            if logDetails:
                print("@"+str(ap) + "/" + str(lines) + " // thisMaster: " + thisMaster + " // " + " thisCE: " + thisCE)

            df_newCasesPerDay_ByShip.loc[
                (df_newCasesPerDay_ByShip[flag_result_date] == incidentDate),
                incidentShip
            ]+=1

            df_newCasesPerDay_ByCpt.loc[
                (df_newCasesPerDay_ByCpt[flag_result_date] == incidentDate),
                thisMaster
            ]+=1

            df_newCasesPerDay_ByCE.loc[
                (df_newCasesPerDay_ByCE[flag_result_date] == incidentDate),
                thisCE
            ] += 1

        df_newCasesPerDay_ByShip.to_csv("db_newIncidentsPerDay_PerShip.csv", sep=";", index=False)
        df_newCasesPerDay_ByCpt.to_csv("db_newIncidentsPerDay_PerMaster.csv", sep=";", index=False)
        df_newCasesPerDay_ByCE.to_csv("db_newIncidentsPerDay_PerCE.csv", sep=";", index=False)

    else:
        print("read available data")
        df_newCasesPerDay_ByShip = pd.read_csv(path_newIncidentsPerDayPerShip, sep=";")
        df_newCasesPerDay_ByCpt = pd.read_csv(path_newIncidentsPerDayPerMaster, sep=";")
        df_newCasesPerDay_ByCE = pd.read_csv(path_newIncidentsPerDayPerCE, sep=";")

    #endregion

    refreshNamesForMasterAndCEPerDay = False
    logDetails = False
    #region get master and ce for each and every day per ship
    if refreshNamesForMasterAndCEPerDay:
        for ap in df_activeMasterDayByDayByName.index:
            thisDate = df_activeMasterDayByDayByName.loc[ap, flag_result_date]
            print("check all master & ce for this date: " + str(thisDate))

            for thisShip in df_activeMasterDayByDayByName.columns:
                if thisShip == flag_result_date:
                    continue

                foundMaster = True
                try:
                    thisMaster = df_masters[
                        (thisDate > df_masters[flag_MasterCE_embarkDate]) &
                        (thisDate <= df_masters[flag_MasterCE_disembarkDate]) &
                        (df_masters[flag_MasterCE_ship] == thisShip)
                        ].iloc[0][flag_MasterCE_name]
                except:
                    if logDetails:
                        print("no master found ")

                    foundMaster = False

                foundCE = True
                try:
                    thisCE = df_ces[
                        (thisDate > df_ces[flag_MasterCE_embarkDate]) &
                        (thisDate <= df_ces[flag_MasterCE_disembarkDate]) &
                        (df_ces[flag_MasterCE_ship] == thisShip)
                        ].iloc[0][flag_MasterCE_name]
                except:
                    if logDetails:
                        print("no ce found ")

                    foundCE = False

                if foundMaster:
                    if logDetails:
                        print("master on " + thisShip + " was " + thisMaster)

                    df_activeMasterDayByDayByName.loc[
                        (df_activeMasterDayByDayByName[flag_result_date] == thisDate),
                        thisShip
                    ] = thisMaster

                if foundCE:
                    if logDetails:
                        print("ce on " + thisShip + " was " + thisCE)

                    df_activeCEDayByDayByName.loc[
                        (df_activeCEDayByDayByName[flag_result_date] == thisDate),
                        thisShip
                    ] = thisCE

        df_activeMasterDayByDayByName.to_csv("df_activeMasterDayByDayByName.csv", sep=";", index=False)
        df_activeCEDayByDayByName.to_csv("df_activeCEDayByDayByName.csv", sep=";", index=False)
    else:
        df_activeMasterDayByDayByName = pd.read_csv(path_nameOfMasterForEachShipPerDay, sep=";")
        df_activeCEDayByDayByName = pd.read_csv(path_nameOfCEForEachShipPerDay, sep=";")
    #endregion

    return \
        df_newCasesPerDay_ByShip, df_newCasesPerDay_ByCpt, df_newCasesPerDay_ByCE,
        df_activeMasterDayByDayByName, df_activeCEDayByDayByName

' #####################################################################################################################'
def func_buildCumulativeView(
    thisDF,
):
    for ap in thisDF.index:
        if ap > 1:
            for thisColumn in thisDF.columns:
                if thisColumn != flag_result_date:
                    thisDF.loc[ap, thisColumn] = thisDF.loc[ap-1, thisColumn] + thisDF.loc[ap, thisColumn]

    thisDF.to_csv("df_result_by_ship_cumulative.csv", sep=";", index = False)

' #####################################################################################################################'
' #####################################################################################################################'
' #####################################################################################################################'


df_masters = pd.DataFrame()
df_ces = pd.DataFrame()
df_incidents = pd.DataFrame()
df_newCasesPerDay_ByShip = pd.DataFrame()
df_newCasesPerDay_ByCpt = pd.DataFrame()
df_newCasesPerDay_ByCE = pd.DataFrame()

df_activeMasterDayByDayByName = pd.DataFrame()
df_activeCEDayByDayByName = pd.DataFrame()

df_newCasesPerDay_ByShip = pd.DataFrame()
df_newCasesPerDay_ByCpt = pd.DataFrame()

df_result_by_ce_cumulative = pd.DataFrame()

df_uptrend_Masters = pd.DataFrame()

df_masters, df_ces, df_incidents = func_readSourceData(df_masters, df_ces, df_incidents)

df_newCasesPerDay_ByShip, df_newCasesPerDay_ByCpt, df_newCasesPerDay_ByCE, \
df_activeMasterDayByDayByName, df_activeCEDayByDayByName = \
    func_loopIncidents(
        df_masters, df_ces, df_incidents,
        df_newCasesPerDay_ByShip, df_newCasesPerDay_ByCpt, df_newCasesPerDay_ByCE,
        df_activeMasterDayByDayByName, df_activeCEDayByDayByName,
        df_uptrend_Masters
    )

func_buildCumulativeView(df_newCasesPerDay_ByShip)