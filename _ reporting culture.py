import pandas as pd
import datetime
from datetime import timedelta

useEventDate = False
refreshAmountOfIncidentsPerDay = False
refreshCumulativeViews = False
refreshNamesForMasterAndCEPerDay = False

refreshShipsPerDayForEach_MASTER = False
refreshShipsPerDayForEach_CE = False

refreshOnboardHistoryMaster = False
refreshOnboardHistoryCE = False
flag_onBoardHistory_dayCount = "DayOnBoard"

path_db_masters = r'E:\001_CMG\HOME\Reporting Culture\_ db MASTERs.csv'
path_db_CEs =  r'E:\001_CMG\HOME\Reporting Culture\_ db CEs.csv'
path_db_incidents =  r'E:\001_CMG\HOME\Reporting Culture\_ db CMG_Incidents.xlsx'

path_newIncidentsPerDayPerShip = r'E:\003_Python_CMG\008_Reporting_Culture\Reporting-Culture\db_newIncidentsPerDay_PerShip.csv'
path_newIncidentsPerDayPerMaster = r'E:\003_Python_CMG\008_Reporting_Culture\Reporting-Culture\db_newIncidentsPerDay_PerMaster.csv'
path_newIncidentsPerDayPerCE = r'E:\003_Python_CMG\008_Reporting_Culture\Reporting-Culture\db_newIncidentsPerDay_PerCE.csv'

path_nameOfMasterForEachShipPerDay = r'E:\003_Python_CMG\008_Reporting_Culture\Reporting-Culture\df_activeMasterDayByDayByName.csv'
path_nameOfCEForEachShipPerDay = r'E:\003_Python_CMG\008_Reporting_Culture\Reporting-Culture\df_activeCEDayByDayByName.csv'

path_nameShipsPerDayForEach_MASTER = r'E:\003_Python_CMG\008_Reporting_Culture\Reporting-Culture\db_whoWasOnBoardOfWhatShipByDay_CPT.csv'
path_nameShipsPerDayForEach_CE = r'E:\003_Python_CMG\008_Reporting_Culture\Reporting-Culture\db_whoWasOnBoardOfWhatShipByDay_CE.csv'

path_nameIncidentDevelopment_MASTER = r'E:\003_Python_CMG\008_Reporting_Culture\Reporting-Culture\db_onBoardHistory_MASTER.csv'
path_nameIncidentDevelopment_CE = r'E:\003_Python_CMG\008_Reporting_Culture\Reporting-Culture\db_onBoardHistory_CE.csv'

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
flag_incidents_dateShoreSideSubmit = "Submitted for Shoreside Review Date"
flag_incidents_Ship = "SHIP"

flag_result_date = "Date"

startDate = datetime.datetime(2017, 1, 1, 0, 0, 0)

testDate = datetime.datetime(2017, 4, 17, 0, 0, 0)

listOfShips = [
    "AIDABELLA",
    "AIDALUNA",
    "AIDAAURA",
    "AIDAMAR",
    "AIDASTELLA",
    "AIDASOL",
    "AIDABLU",
    "AIDACARA",
    "AIDAPERLA",
    "AIDANOVA",
    "AIDAPRIMA",
    "AIDADIVA",
    "AIDAVITA",
    "AIDAMIRA",
    "DELIZIOSA",
    "ATLANTICA",
    "SERENA",
    "FASCINOSA",
    "MEDITERRANEA",
    "SMERALDA",
    "NEOROMANTICA",
    "NEORIVIERA",
    "NEOCLASSICA",
    "PACIFICA",
    "MAGICA",
    "FORTUNA",
    "VICTORIA",
    "LUMINOSA",
    "FAVOLOSA",
    "DIADEMA",
    "FIRENZE",
    "VENEZIA",
]

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
    df_masters[flag_MasterCE_embarkDate] = pd.to_datetime(df_masters[flag_MasterCE_embarkDate], format='%d.%m.%Y')
    df_masters[flag_MasterCE_disembarkDate] = pd.to_datetime(df_masters[flag_MasterCE_disembarkDate], format='%d.%m.%Y')

    df_ces[flag_MasterCE_embarkDate] = pd.to_datetime(df_ces[flag_MasterCE_embarkDate], format='%d.%m.%Y')
    df_ces[flag_MasterCE_disembarkDate] = pd.to_datetime(df_ces[flag_MasterCE_disembarkDate], format='%d.%m.%Y')

    df_incidents[flag_incidents_date] = pd.to_datetime(df_incidents[flag_incidents_date], format='%Y-%m-%d')

    df_incidents.loc[
        (df_incidents[flag_incidents_dateShoreSideSubmit] == "NaT"),
        flag_incidents_dateShoreSideSubmit
    ] = df_incidents.loc[
        (df_incidents[flag_incidents_dateShoreSideSubmit] == "NaT"),
        flag_incidents_date
    ]

    df_incidents.loc[
        (df_incidents[flag_incidents_dateShoreSideSubmit] == "InfoSHIP Statement not issued, despite being chased"),
        flag_incidents_dateShoreSideSubmit
    ] = df_incidents.loc[
        (df_incidents[flag_incidents_dateShoreSideSubmit] == "InfoSHIP Statement not issued, despite being chased"),
        flag_incidents_date
    ]

    df_incidents[flag_incidents_dateShoreSideSubmit] = pd.to_datetime(df_incidents[flag_incidents_dateShoreSideSubmit],
                                                       format='%Y-%m-%d')  # .strftime('%Y.%m.%d')#.astype('datetime64[ns]')
    #endregion

    print(df_masters.head(5))
    print(df_ces.head(5))
    print(df_incidents.head(5))

    return df_masters, df_ces, df_incidents

' #####################################################################################################################'
def func_loopIncidents(
    df_masters, df_ces, df_incidents,
    flag_eventReferenceTime,
    df_newCasesPerDay_ByShip,
    df_newCasesPerDay_ByCpt,
    df_newCasesPerDay_ByCE,
    df_activeMasterDayByDayByName, df_activeCEDayByDayByName,
    df_whoWasOnBoardOfWhatShipByDay_CPT, df_whoWasOnBoardOfWhatShipByDay_CE,
    df_onBoardHistory_MASTER, df_onBoardHistory_CE
):
    sr = pd.Series(pd.date_range(startDate, periods=1735, freq='D'))

    #region prepare daily structure
    dayCount = 0
    for thisDay in sr.index:
        dayCount+=1

        df_onBoardHistory_MASTER = df_onBoardHistory_MASTER.append(
            {
                flag_onBoardHistory_dayCount: dayCount
            }, ignore_index=True
        )

        df_onBoardHistory_CE = df_onBoardHistory_CE.append(
            {
                flag_onBoardHistory_dayCount: dayCount
            }, ignore_index=True
        )

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

            df_whoWasOnBoardOfWhatShipByDay_CPT = df_whoWasOnBoardOfWhatShipByDay_CPT.append(
                {
                    flag_result_date: thisDate
                }, ignore_index=True
            )

            df_whoWasOnBoardOfWhatShipByDay_CE = df_whoWasOnBoardOfWhatShipByDay_CE.append(
                {
                    flag_result_date: thisDate
                }, ignore_index=True
            )
    #endregion

    #region prepare blank columns by ship
    df_newCasesPerDay_ByShip = df_newCasesPerDay_ByShip.reset_index(drop=True)
    for thisShip in df_incidents[flag_incidents_Ship].unique():
        df_newCasesPerDay_ByShip[thisShip] = 0
        df_activeMasterDayByDayByName[thisShip] = ""
        df_activeCEDayByDayByName[thisShip] = ""


    print(df_newCasesPerDay_ByShip.head(5))
    #endregion

    #region prepare header by CPT
    df_newCasesPerDay_ByCpt = df_newCasesPerDay_ByCpt.reset_index(drop=True)
    for thisCPT in df_masters[flag_MasterCE_name].unique():
        df_newCasesPerDay_ByCpt[thisCPT] = 0
        df_whoWasOnBoardOfWhatShipByDay_CPT[thisCPT] = 0
        df_onBoardHistory_MASTER[thisCPT] = 0

    print(df_newCasesPerDay_ByCpt.head(5))
    #endregion

    #region prepare header by CE
    df_newCasesPerDay_ByCE = df_newCasesPerDay_ByCE.reset_index(drop=True)
    for thisCE in df_ces[flag_MasterCE_name].unique():
        df_newCasesPerDay_ByCE[thisCE] = 0
        df_whoWasOnBoardOfWhatShipByDay_CE[thisCE] = 0
        df_onBoardHistory_CE[thisCE] = 0

    print(df_newCasesPerDay_ByCE.head(5))
    #endregion

    logDetails = False
    #region create dataframe with new cases per day by MASTER CE SHIP
    if refreshAmountOfIncidentsPerDay:
        lines = df_incidents.index.max()
        for ap in df_incidents.index:
            try:
                incidentDate = df_incidents.loc[ap, flag_eventReferenceTime].strftime('%Y-%m-%d')
            except:
                print("that is no valid date: " +str(df_incidents.loc[ap, flag_eventReferenceTime]))
                continue

            incidentShip = df_incidents.loc[ap, flag_incidents_Ship]
            print("incident @ " + str(incidentDate) + " on board " + incidentShip)

            try:
                thisMaster = df_masters[
                    (incidentDate >= df_masters[flag_MasterCE_embarkDate]) &
                    (incidentDate <= df_masters[flag_MasterCE_disembarkDate]) &
                    (df_masters[flag_MasterCE_ship] == incidentShip)
                ].iloc[0][flag_MasterCE_name]
            except:
                if logDetails:
                    print("no master found ")

            try:
                thisCE = df_ces[
                    (incidentDate >= df_ces[flag_MasterCE_embarkDate]) &
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
                        (thisDate >= df_masters[flag_MasterCE_embarkDate]) &
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
                        (thisDate >= df_ces[flag_MasterCE_embarkDate]) &
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

    logDetails = True
    #region get ships name for every day for each master
    if refreshShipsPerDayForEach_MASTER:
        for ap in df_masters.index:
            thisRun_ship = df_masters.loc[ap, flag_MasterCE_ship]
            if thisRun_ship not in listOfShips:
                continue

            thisRun_master = df_masters.loc[ap, flag_MasterCE_name]
            thisRun_onDay = df_masters.loc[ap, flag_MasterCE_embarkDate].strftime('%Y-%m-%d')
            thisRun_offDay = df_masters.loc[ap, flag_MasterCE_disembarkDate].strftime('%Y-%m-%d')

            if logDetails:
                print("fill master for this run " + "\n" +
                      "thisRun_master: " + thisRun_master + " // " + thisRun_ship + " // " + str(thisRun_onDay) + " - " + str(thisRun_offDay))

            if thisRun_master in df_whoWasOnBoardOfWhatShipByDay_CPT.columns:
                df_whoWasOnBoardOfWhatShipByDay_CPT.loc[
                    (df_whoWasOnBoardOfWhatShipByDay_CPT[flag_result_date] >= thisRun_onDay) &
                    (df_whoWasOnBoardOfWhatShipByDay_CPT[flag_result_date] <= thisRun_offDay),
                    thisRun_master
                ] = thisRun_ship

        df_whoWasOnBoardOfWhatShipByDay_CPT.to_csv("db_whoWasOnBoardOfWhatShipByDay_CPT.csv", sep = ";", index=False)
    else:
        df_whoWasOnBoardOfWhatShipByDay_CPT = pd.read_csv(path_nameShipsPerDayForEach_MASTER, sep=";")
    #endregion

    logDetails = True
    # region get ships name for every day for each master
    if refreshShipsPerDayForEach_CE:
        for ap in df_ces.index:
            thisRun_ship = df_ces.loc[ap, flag_MasterCE_ship]
            if thisRun_ship not in listOfShips:
                continue

            thisRun_ce = df_ces.loc[ap, flag_MasterCE_name]
            thisRun_onDay = df_ces.loc[ap, flag_MasterCE_embarkDate].strftime('%Y-%m-%d')
            thisRun_offDay = df_ces.loc[ap, flag_MasterCE_disembarkDate].strftime('%Y-%m-%d')

            if logDetails:
                print("fill ce for this run " + "\n" +
                      "thisRun_ce: " + thisRun_ce + " // " + thisRun_ship + " // " + str(
                    thisRun_onDay) + " - " + str(thisRun_offDay))

            if thisRun_ce in df_whoWasOnBoardOfWhatShipByDay_CE.columns:
                df_whoWasOnBoardOfWhatShipByDay_CE.loc[
                    (df_whoWasOnBoardOfWhatShipByDay_CE[flag_result_date] >= thisRun_onDay) &
                    (df_whoWasOnBoardOfWhatShipByDay_CE[flag_result_date] <= thisRun_offDay),
                    thisRun_ce
                ] = thisRun_ship

        df_whoWasOnBoardOfWhatShipByDay_CE.to_csv("db_whoWasOnBoardOfWhatShipByDay_CE.csv", sep=";", index=False)
    else:
        df_whoWasOnBoardOfWhatShipByDay_CE = pd.read_csv(path_nameShipsPerDayForEach_CE, sep=";")
    # endregion

    logDetails = False
    #region fill dataframe with incident development during board stays
    if refreshOnboardHistoryMaster:
        for thisMaster in df_whoWasOnBoardOfWhatShipByDay_CPT.columns.unique():
            print("get history over time for thisMaster " + thisMaster)
            cntDaysOnBoard = 0
            totalIncidentsThisCpt = 1
            for ap in df_whoWasOnBoardOfWhatShipByDay_CPT.index:
                if logDetails:
                    print("ship @ line " + str(ap) + " = " + str(df_whoWasOnBoardOfWhatShipByDay_CPT.loc[ap, thisMaster]))
                if df_whoWasOnBoardOfWhatShipByDay_CPT.loc[ap, thisMaster] not in listOfShips:
                    continue

                thisDate = df_whoWasOnBoardOfWhatShipByDay_CPT.loc[ap, flag_result_date]
                if logDetails:
                    print(thisMaster + " was on board @ " + str(thisDate))

                cntDaysOnBoard += 1

                totalIncidentsThisCpt = \
                    totalIncidentsThisCpt + \
                        df_newCasesPerDay_ByCpt.loc[
                            (df_newCasesPerDay_ByCpt[flag_result_date] == thisDate),
                            thisMaster
                        ].sum()

                print("totalIncidentsThisCpt " + str(totalIncidentsThisCpt))

                df_onBoardHistory_MASTER.loc[cntDaysOnBoard, thisMaster] = totalIncidentsThisCpt

        df_onBoardHistory_MASTER.to_csv("db_onBoardHistory_MASTER.csv", sep=";", index=False)
    else:
        df_onBoardHistory_MASTER = pd.read_csv(path_nameIncidentDevelopment_MASTER, sep=";")
    #endregion

    logDetails = False
    # region fill dataframe with incident development during board stays
    if refreshOnboardHistoryCE:
        for thisCE in df_whoWasOnBoardOfWhatShipByDay_CE.columns.unique():
            print("get history over time for thisCE " + thisCE)
            cntDaysOnBoard = 0
            totalIncidentsThisCE = 1
            for ap in df_whoWasOnBoardOfWhatShipByDay_CE.index:
                if logDetails:
                    print(
                        "ship @ line " + str(ap) + " = " + str(df_whoWasOnBoardOfWhatShipByDay_CE.loc[ap, thisCE]))
                if df_whoWasOnBoardOfWhatShipByDay_CE.loc[ap, thisCE] not in listOfShips:
                    continue

                thisDate = df_whoWasOnBoardOfWhatShipByDay_CE.loc[ap, flag_result_date]
                if logDetails:
                    print(thisCE + " was on board @ " + str(thisDate))

                cntDaysOnBoard += 1

                totalIncidentsThisCE = \
                    totalIncidentsThisCE + \
                    df_newCasesPerDay_ByCE.loc[
                        (df_newCasesPerDay_ByCE[flag_result_date] == thisDate),
                        thisCE
                    ].sum()

                if logDetails:
                    print("totalIncidentsThisCE " + str(totalIncidentsThisCE))

                df_onBoardHistory_CE.loc[cntDaysOnBoard, thisCE] = totalIncidentsThisCE

        df_onBoardHistory_CE.to_csv("db_onBoardHistory_CE.csv", sep=";", index=False)
    else:
        df_onBoardHistory_CE = pd.read_csv(path_nameIncidentDevelopment_CE, sep=";")
    # endregion

    return \
        df_newCasesPerDay_ByShip, df_newCasesPerDay_ByCpt, df_newCasesPerDay_ByCE, \
        df_activeMasterDayByDayByName, df_activeCEDayByDayByName

' #####################################################################################################################'
def func_buildCumulativeView(
    thisDF,
    byWhat
):
    print("buil cumulative view for " + byWhat)

    for ap in thisDF.index:
        if ap > 1:
            for thisColumn in thisDF.columns:
                if thisColumn != flag_result_date:
                    thisDF.loc[ap, thisColumn] = thisDF.loc[ap-1, thisColumn] + thisDF.loc[ap, thisColumn]

    thisDF.to_csv("df_incidentsPerDayCumulative_"+ byWhat + ".csv", sep=";", index = False)

' #####################################################################################################################'
' #####################################################################################################################'
' #####################################################################################################################'

if useEventDate:
    flag_eventReferenceTime = flag_incidents_date
else:
    flag_eventReferenceTime = flag_incidents_dateShoreSideSubmit

df_masters = pd.DataFrame()
df_ces = pd.DataFrame()
df_incidents = pd.DataFrame()

df_newCasesPerDay_ByShip = pd.DataFrame()
df_newCasesPerDay_ByCpt = pd.DataFrame()
df_newCasesPerDay_ByCE = pd.DataFrame()

df_activeMasterDayByDayByName = pd.DataFrame()
df_activeCEDayByDayByName = pd.DataFrame()

df_whoWasOnBoardOfWhatShipByDay_CPT = pd.DataFrame()
df_whoWasOnBoardOfWhatShipByDay_CE = pd.DataFrame()

df_onBoardHistory_MASTER = pd.DataFrame()
df_onBoardHistory_CE = pd.DataFrame()

df_masters, df_ces, df_incidents = func_readSourceData(df_masters, df_ces, df_incidents)

df_newCasesPerDay_ByShip, df_newCasesPerDay_ByCpt, df_newCasesPerDay_ByCE, \
df_activeMasterDayByDayByName, df_activeCEDayByDayByName = \
    func_loopIncidents(
        df_masters, df_ces, df_incidents,
        flag_eventReferenceTime,
        df_newCasesPerDay_ByShip, df_newCasesPerDay_ByCpt, df_newCasesPerDay_ByCE,
        df_activeMasterDayByDayByName, df_activeCEDayByDayByName,
        df_whoWasOnBoardOfWhatShipByDay_CPT, df_whoWasOnBoardOfWhatShipByDay_CE,
        df_onBoardHistory_MASTER, df_onBoardHistory_CE
    )


if refreshCumulativeViews:
    func_buildCumulativeView(df_newCasesPerDay_ByShip, "byShip")
    func_buildCumulativeView(df_newCasesPerDay_ByCpt, "byCpt")
    func_buildCumulativeView(df_newCasesPerDay_ByCE, "byCE")

print("damn. nice. done")