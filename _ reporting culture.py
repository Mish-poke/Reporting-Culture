import pandas as pd
import datetime
from datetime import timedelta

path_db_masters = r'E:\001_CMG\HOME\Reporting Culture\_ db MASTERs.csv'
path_db_CEs =  r'E:\001_CMG\HOME\Reporting Culture\_ db CEs.csv'
path_db_incidents =  r'E:\001_CMG\HOME\Reporting Culture\_ db CMG_Incidents.xlsx'

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
    df_masters[flag_MasterCE_embarkDate] = df_masters[flag_MasterCE_embarkDate].astype('datetime64[ns]')
    df_masters[flag_MasterCE_disembarkDate] = df_masters[flag_MasterCE_disembarkDate].astype('datetime64[ns]')

    df_ces[flag_MasterCE_embarkDate] = df_ces[flag_MasterCE_embarkDate].astype('datetime64[ns]')
    df_ces[flag_MasterCE_disembarkDate] = df_ces[flag_MasterCE_disembarkDate].astype('datetime64[ns]')

    df_incidents[flag_incidents_date] = df_incidents[flag_incidents_date].astype('datetime64[ns]')
    #endregion

    print(df_masters.head(5))
    print(df_ces.head(5))
    print(df_incidents.head(5))

    return df_masters, df_ces, df_incidents

' #####################################################################################################################'
def func_loopIncidents(
    df_masters,
    df_ces,
    df_incidents,
    df_result_by_ship
):
    sr = pd.Series(pd.date_range(startDate, periods=999, freq='D'))

    for thisDay in sr.index:
        if thisDay < sr.index.max():
            thisDate = (startDate + timedelta(days=thisDay)).strftime('%Y-%m-%d')
            # print("thisDay: " + str(thisDate))

            df_result_by_ship = df_result_by_ship.append(
                {
                    flag_result_date: thisDate
                }, ignore_index=True
            )

    for thisShip in df_incidents[flag_incidents_Ship].unique():
        df_result_by_ship[thisShip] = ""

    print(df_result_by_ship.head(5))

    lines = df_incidents.index.max()
    for ap in df_incidents.index:
        incidentDate = df_incidents.loc[ap, flag_incidents_date] #.strftime('%d-%m-%Y')
        incidentShip = df_incidents.loc[ap, flag_incidents_Ship]
        print("incident @ " + str(incidentDate) + " on board " + incidentShip)

        # if incidentShip[:5] == "COSTA":
        #     incidentShip = incidentShip[6:]
        #     print("changed the costa ship to " + incidentShip)

        try:
            thisMaster = df_masters[
                (incidentDate > df_masters[flag_MasterCE_embarkDate]) &
                (incidentDate <= df_masters[flag_MasterCE_disembarkDate]) &
                (incidentShip == df_masters[flag_MasterCE_ship])
            ].iloc[0][flag_MasterCE_name]
        except:
            print("no master found ")

        try:
            thisCE = df_ces[
                (incidentDate > df_ces[flag_MasterCE_embarkDate]) &
                (incidentDate <= df_ces[flag_MasterCE_disembarkDate]) &
                (incidentShip == df_ces[flag_MasterCE_ship])
                ].iloc[0][flag_MasterCE_name]
        except:
            print("no ce found ")

        print("@"+str(ap) + "/" + str(lines) + " // thisMaster: " + thisMaster + " // " + " thisCE: " + thisCE)

        df_result_by_ship.loc[
            (df_result_by_ship[flag_result_date] == incidentDate),
            incidentShip
        ] = 27

    df_result_by_ship.to_csv("df_result_by_ship.csv", sep=";")

    return df_result_by_ship

' #####################################################################################################################'
' #####################################################################################################################'
' #####################################################################################################################'


df_masters = pd.DataFrame()
df_ces = pd.DataFrame()
df_incidents = pd.DataFrame()
df_result_by_ship = pd.DataFrame()

df_masters, df_ces, df_incidents = func_readSourceData(df_masters, df_ces, df_incidents)

df_result_by_ship = func_loopIncidents(df_masters, df_ces, df_incidents, df_result_by_ship)