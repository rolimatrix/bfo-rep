import pandas as pd
import datetime
import time
import os
import json
import traceback
from task_analyse import BO_Task_Analyse

def neueADMDaten():
    filesinDir = os.listdir(path=os.curdir)
    str_match = list(filter(lambda x: 'am_vvm_area.csv' in x, filesinDir))
    file_AREA=str_match[0]
    str_match = list(filter(lambda x: 'am_nvt_area.csv' in x, filesinDir))
    file_NVT = str_match[0]
    str_match = list(filter(lambda x: 'am_fibre_on_location.csv' in x, filesinDir))
    file_FOL = str_match[0]

    if file_NVT=="" or file_FOL=="" or file_AREA=="":
        return "No Files"

    try:
        fields = ['number', 'name', 'initiative', 'status', 'tenant_id','internal_expansion_decision_date']
        AREA = pd.read_csv(file_AREA, skipinitialspace=True, usecols=fields,encoding='utf-8', sep=';',
                           converters={"initiative": str, 'number': str, 'tenant_id': str})
        AREA.rename(columns={'number': 'am_vvm_area_number_fk'}, inplace=True)

        fields = ['id', 'we', 'ge', 'zb', 'am_nvt_area_bid_fk', 'am_vvm_area_number_fk', 'kls_id', 'installation_status',
                  'reason_for_no_constr','build_agree_state', 'production_mode', 'is_development_location', 'has_rule_violation']
        FOL = pd.read_csv(file_FOL, skipinitialspace=True, usecols=fields,encoding='utf-8', sep=';',
                          converters={"we": str, "ge": str, "zb": str, "kls_id": str, 'am_nvt_area_bid_fk': str,
                                      'am_vvm_area_number_fk': str, "installation_status": str})
        FOL.astype({'id': 'int64'}).dtypes
        fields = ['bid', 'asb', 'name', 'onkz']
        NVT = pd.read_csv(file_NVT, skipinitialspace=True, usecols=fields,encoding='utf-8', sep=';',
                          converters={"bid": str, "asb": str, "name": str, "onkz": str})
        NVT.rename(columns={'bid': 'am_nvt_area_bid_fk', 'name': 'nvt_name'}, inplace=True)
        FOL = pd.merge(AREA, FOL, how="left", on="am_vvm_area_number_fk")
        FOL = FOL[FOL["name"].str.contains('WBV_') == False]
        FOL = pd.merge(FOL, NVT, how="left", on="am_nvt_area_bid_fk")

        FOL = FOL[(FOL['initiative'] != "WHOLE_BUY_BSA") & (FOL['initiative'] != "WHOLE_BUY_OAL")]
        FOL.to_csv("area_fol.csv", index=False, sep=';', encoding='utf-8')
    except Exception as e:

        return traceback.format_exc()

def bulk_rep_name(region):
    year=datetime.date.today().isocalendar()[0]
    kw = datetime.date.today().isocalendar()[1]
    if region=="O":
        return "TNL Ost_Bulk_Rep_{}_KW_{}.csv".format(year,kw)
    elif region=="W":
        return "TNL West_Bulk_Rep_{}_KW_{}.csv".format(year,kw)
    elif region== "N":
        return "TNL Nord_Bulk_Rep_{}_KW_{}.csv".format(year,kw)
    elif region== "S":
        return "TNL Sued_Bulk__{}_KW_{}.csv".format(year,kw)
    elif region =="SW":
        return "TNL SuedWest_Bulk_Rep_{}_KW_{}.csv".format(year,kw)
    elif region =="GF+":
        return "GF Plus_Bulk_Rep_{}_KW_{}.csv".format(year,kw)
    elif region=="BUND":
        return "Bundesweiter_Bulk_Rep_{}_KW_{}.csv".format(year, kw)

def in_method(given, sub):
    return sub in given

def format_bfo_report():
    BFO_REPORT = pd.DataFrame(columns=['KLS-ID','Area_Nr','Area_Name','Rollout Verantwortung','Bulk_Prefix','Projektname','Bulk Projektnummer','Erstellung','Ausbaustatus',
                                       'FOL_ID','NVt-Bezeichner','zu bauen bis','Auftragnehmer NE3','Auftragnehmer NE4','Technologie',
                                       'Ausbauart','Anzahl Anschluesse','TA_GEBAUT','TA_NOCH_NICHT_GEBAUT','TA_IN_BAU','Status Bulk Projekt','Status Adresse','next Step','Fehler Nachricht PON Planning','Bulk Order ID','ProdTaskDict','projNe3','projNe4'])


    return BFO_REPORT
def adressen():
    ADR = pd.read_csv("address.csv", skipinitialspace=True, delimiter=';', usecols=fields, encoding='utf-8')
    ADR.set_index('kls_id', inplace=True)
    return ADR
def partyLesen():
    PARTY = pd.DataFrame()
    fields = ['partyid','trading_name','name_type']
    filesinDir = os.listdir(path=os.curdir)
    str_match = list(filter(lambda x: 'pm_organization.csv' in x, filesinDir))
    if str_match:
        file=str_match[0]
    else:
        return PARTY
    PARTY = pd.read_csv(file, skipinitialspace=True, delimiter=';', usecols=fields, encoding='utf-8',converters = {"partyid":str})
    PARTY.set_index('partyid', inplace=True)
    return PARTY

def ROP(PARTY,region):
    if region=="O":
        ROP_PTI= "PTI Ost"
        ROP_APL="APL Ost"
    elif region=="W":
        ROP_PTI= "PTI West"
        ROP_APL = "APL West"
    elif region== "N":
        ROP_PTI= "PTI Nord"
        ROP_APL = "APL Nord"
    elif region== "S":
        ROP_PTI= "PTI Süd"
        ROP_APL = "APL Süd"
    elif region =="SW":
        ROP_PTI= "PTI Südwest"
        ROP_APL = "APL Südwest"
    elif region =="GF+":
        ROP_PTI = "Glasfaser"
        ROP_APL= "ROLLOUT_PARTNER"

    if ROP_PTI=="Glasfaser":
        ROP = PARTY[(PARTY['trading_name'].str.contains(ROP_PTI, na=False)) & (PARTY['name_type'].str.contains(ROP_APL, na=False))]
    else:
        ROP = PARTY[(PARTY['trading_name'].str.contains(ROP_PTI, na=False) )| (PARTY['trading_name'].str.contains(ROP_APL, na=False))]


    #bei Sued sind auch die SuedWest PTI dabei. Das muss verhindert werden
    if ROP_PTI =="PTI Süd":
        ROP = ROP[(ROP['trading_name'].str.contains("PTI Südwest")==False) & (ROP['trading_name'].str.contains("APL Südwest")==False)]

    ROP= Area_PTI(ROP)
    return ROP

def Area_PTI(PTI):
    LOC_RESPONSE=pd.DataFrame()
    fields = ['organization_id', 'vvm_area_number']
    filesinDir = os.listdir(path=os.curdir)
    str_match = list(filter(lambda x: 'pm_location_responsibility.csv' in x, filesinDir))
    if str_match:
        file = str_match[0]
    else:
        return
    LOC_RESPONSE = pd.read_csv(file, skipinitialspace=True, delimiter=';',
                                       usecols=fields,encoding='utf-8',
                                       converters={"organization_id": str})
    LOC_RESPONSE.set_index('organization_id', inplace=True)
    LOC_RESPONSE = PTI.join(LOC_RESPONSE)
    LOC_RESPONSE= Giga_Area_Namen_ermitteln(LOC_RESPONSE)
    return LOC_RESPONSE
def Giga_Area_Namen_ermitteln(LOC_RESP):
    filesinDir = os.listdir(path=os.curdir)
    str_match = list(filter(lambda x: 'am_vvm_area.csv' in x, filesinDir))
    file_AREA=str_match[0]
    fields = ['number', 'name']
    AREA = pd.read_csv(file_AREA, skipinitialspace=True, usecols=fields, encoding='utf-8', sep=';',
                       converters={'number': str})
    AREA.rename(columns={'number': 'am_vvm_area_number_fk'}, inplace=True)
    AREA.set_index('am_vvm_area_number_fk',inplace=True)
    LOC_RESP['area_name']=""
    for row in LOC_RESP.itertuples():
        area_ID= getattr(row,'vvm_area_number')
        try:
            Area_Name= AREA.loc[area_ID]
            LOC_RESP.loc[LOC_RESP.vvm_area_number==area_ID,'area_name']=Area_Name[0]
        except:
            continue
    return LOC_RESP
def allBFO_read(ROP):
    filtered_BFO= pd.DataFrame()
    relevant_Area= ROP['vvm_area_number']
    filesinDir = os.listdir(path=os.curdir)
    str_match = list(filter(lambda x: 'bfo-engine_building-order.csv' in x, filesinDir))
    if str_match:
        file = str_match[0]
    else:
        return filtered_BFO

    BFO = pd.read_csv(file, skipinitialspace=True, sep=';', encoding='utf-8',dtype={'ne4_construction_ordered':str, 'ne4_number_ordered':str,'fk_partyid_ne3_supplier':str,'fk_partyid_ne4_supplier':str,
                                                                                    'fk_ne3_supplier_project_id':str,'fk_ne4_supplier_project_id':str})
    BFO.astype({'fk_fibre_on_location_id':'int64'}).dtypes #,'building_order_id':'int64','subtask_order_id':'int64'
    BFO= BFO[(BFO['building_order_state'] != "CANCELED")]
    BFO.set_index('bulk_id', inplace=True)
    BFO['bulk_id']= BFO.index
    filtered_BFO = BFO[BFO.fk_giga_area_number.isin(relevant_Area)]

    return filtered_BFO

def bfo_projecte(BFO,region):
    BFO_PROJ= BFO['bulk_id']
    BFO_PROJ= BFO_PROJ.drop_duplicates()
    #FileName = bulk_rep_name(region)
    #BFO_PROJ.to_csv("Projekte_{}".format(FileName), index=False, sep=';', encoding='utf-8')
    return BFO_PROJ

def bfo_liste(BFO,BFO_PROJ,PARTY,ROP_LOK,region):
    BFO_REP= format_bfo_report()

    for row in BFO_PROJ.iteritems():
        bulk_projID = row[0]

        try:
            # Holle nun fuer das Indexierte Bulk Projekt alle Positionen
            ALL_BO_POS= BFO.loc[bulk_projID]
        except Exception as e:
            print("Error keine Bulk Einträge gefunden für Bulk Projekt {}".format(bulk_projID))
            continue
        #da je FOL aus dem BFO Abzug zu einer BO x BO Task vorliegen und ich nur eine Zeile auswerten
        #moechte merke ich mir mit dieser Liste die FOL ID
        FOL_LISTE=[]

        try:
            #Es koennte sein das ein Series Objekt aus dem vorherigem Try entstanden ist
            if in_method(str(type(ALL_BO_POS)),"Series"):
                # Aufwaendige Transformation des Series wo Objekt in den Dataframe
                col = list(ALL_BO_POS.index)
                val = list(ALL_BO_POS.values)
                werte = dict(zip(col, val))
                ALL_BO_POS = pd.DataFrame(columns=list(werte.keys()))
                for k, v in werte.items():
                    ALL_BO_POS.at[0, k] = v

            for i, row in ALL_BO_POS.iterrows():
                fol_id = row['fk_fibre_on_location_id']
                if fol_id in FOL_LISTE:
                    continue
                else:
                    FOL_LISTE.append(fol_id)
                    bulk_state= row['bulk_installation_order_state']
                    bo_state = row['building_order_state']
                    vollausbau = row['ne4_construction_ordered']

                    ALL_BOS_TASK = ALL_BO_POS[['subtask_type','subtask_state','subtask_order_id','error_content','fk_fibre_on_location_id']]
                    #only Task for actual FOL
                    ALL_BOS_TASK= ALL_BOS_TASK[(ALL_BOS_TASK['fk_fibre_on_location_id']==fol_id)]
                    Prod_Task = ALL_BOS_TASK[(ALL_BOS_TASK['subtask_type']=="EXPLORATION")|(ALL_BOS_TASK['subtask_type']=="GFTA")|(ALL_BOS_TASK['subtask_type']=="ONEBOX")
                                             |(ALL_BOS_TASK['subtask_type']=="DPU")]

                    #brauche ich da ich spaeter ueber diese Task ID mir die Meilensteine hole
                    task_IdDict={"EXPLORATION":0,"ONEBOX":0,"GFTA":0,"DPU":0}
                    for iProdTask, rowProdTask in Prod_Task.iterrows():
                        if rowProdTask['subtask_type']=='EXPLORATION':
                            task_IdDict["EXPLORATION"]= rowProdTask['subtask_order_id']
                        elif rowProdTask['subtask_type']=='ONEBOX':
                            task_IdDict["ONEBOX"] = rowProdTask['subtask_order_id']
                        elif rowProdTask['subtask_type']=='GFTA':
                            task_IdDict["GFTA"] = rowProdTask['subtask_order_id']
                        elif rowProdTask['subtask_type']=='DPU':
                            task_IdDict["DPU"] = rowProdTask['subtask_order_id']
                        else:
                            continue

                    kls_id = row['fk_kls_id']
                    technik = row['technology']
                    ### with that Function i analyze Status of Task for one BO.
                    next_step=""
                    BO_RES = BO_Task_Analyse(ALL_BOS_TASK, bo_state, technik,bulk_state)
                    next_step=BO_RES[0]
                    GIGATA_DICT=BO_RES[1]
                    if len(BO_RES) ==3:
                        ErrorContent=BO_RES[2]
                    else:
                        ErrorContent=""

                    GFTA_GEBAUT= GIGATA_DICT["GFTA_GEBAUT"]
                    GFTA_NOCH_NICHT_GEBAUT=GIGATA_DICT["GFTA_NOCH_NICHT_GEBAUT"]
                    GFTA_IN_BAU=GIGATA_DICT["GFTA_IN_BAU"]

                try:
                    kls_id =row['fk_kls_id']
                    area_ID= row['fk_giga_area_number']
                    try:
                        ROP_ZEILE= ROP_LOK[(ROP_LOK['vvm_area_number']==area_ID)]
                        rop_name,areaName=ROP_Namen_ermittelln(ROP_ZEILE)
                    except Exception as e:
                        print("ROP Zeile nicht gefunden")
                        pass

                    ne3_suppl= row['fk_partyid_ne3_supplier']

                    if not pd.isna(ne3_suppl):
                        try:
                            partyN3 = PARTY.loc[ne3_suppl]
                            partyN3= partyN3[0]
                        except:
                            partyN3="Supplier NE3 mit Party ID {} nicht in der Party Referenz".format(ne3_suppl)
                    else:
                        partyN3=""
                    ne4_suppl = row['fk_partyid_ne4_supplier']
                    if not pd.isna(ne4_suppl):
                        try:
                            partyN4 = PARTY.loc[ne4_suppl]
                            partyN4 = partyN4[0]
                        except:
                            partyN4="Supplier NE4 mit Party ID {} nicht in der Party Referenz".format(ne4_suppl)
                    else:
                        partyN4 = ""

                    BO_ID=row['building_order_id']
                    bulk_name = row['bulk_name']
                    Bulk_Pref= row['construction_type']
                    if Bulk_Pref=='NEW':
                        Bulk_Pref='NBG'
                    elif Bulk_Pref=='EXISTING':
                        Bulk_Pref='BES'

                    erstellung = row['creation_date']
                    projNe3= row['fk_ne3_supplier_project_id']
                    projNe4=row['fk_ne4_supplier_project_id']

                    erstellung = row['creation_date']
                    if not pd.isna(erstellung):
                        erstellung = erstellung[0:10]
                    vollausbau = row['ne4_construction_ordered']
                    anzDosen= row['ne4_number_ordered']


                    if vollausbau=='t' or vollausbau==1:
                        if technik=="FTTH":
                            vollausbau= "Vollverglasung"
                        elif technik=="FTTB":
                            vollausbau = "FTTB Full"
                        else:
                            vollausbau="nicht definierbar"
                    elif vollausbau=='f' or vollausbau==0:
                        vollausbau = "Ha Only"

                    MST=""
                    Eintrag = pd.DataFrame.from_records([{'Area_Nr':area_ID,'Area_Name':areaName,'Bulk_Prefix':Bulk_Pref,'Projektname': bulk_name,'Rollout Verantwortung':rop_name,
                                                          'Bulk Projektnummer':bulk_projID,'Erstellung':erstellung,'KLS-ID':kls_id,'FOL_ID':fol_id,'Auftragnehmer NE3': partyN3,'Auftragnehmer NE4': partyN4,
                               'Technologie':technik,'Ausbauart':vollausbau,'Anzahl Anschluesse':anzDosen,'Status Bulk Projekt':bulk_state,'Status Adresse':bo_state,
                                                          'zu bauen bis':MST,'Bulk Order ID':BO_ID,'next Step':next_step,'Fehler Nachricht PON Planning':ErrorContent,'TA_GEBAUT':GFTA_GEBAUT,
                                                          'TA_NOCH_NICHT_GEBAUT':GFTA_NOCH_NICHT_GEBAUT,'TA_IN_BAU':GFTA_IN_BAU,'ProdTaskDict':json.dumps(task_IdDict),'projNe3':projNe3,'projNe4':projNe4}])

                    BFO_REP = pd.concat([BFO_REP, Eintrag])
                    print('Zeile {} hinzugefuegt'.format(len(BFO_REP)))
                except Exception as e:
                    import traceback
                    print(traceback.format_exc())
                    continue

            # print("Fehler in Aufbau Zeile fuer Projekt {} passiert".format(str(i)))


        except:
            print("Fehler in Schleife all BO fuer Projekt {} passiert".format(i))

    FileName= bulk_rep_name(region)
    try:
        BFO_REP.to_csv(FileName, index=False, sep=';', encoding='utf-8')
    except Exception as e:
        # print("Fehler in Schleife all BO fuer Projekt {} passiert".format(i))
        print(traceback.format_exc())
        print(f"{e}")


    return BFO_REP

def ROP_Namen_ermittelln(ROP_ZEILE):
    areaName = ROP_ZEILE.iloc[0, 3]
    rop_name = ROP_ZEILE.iloc[0, 0]
    rop2 = ""
    if len(ROP_ZEILE)>1:
        for i in range(1, len(ROP_ZEILE)):
            rop2 = ROP_ZEILE.iloc[i, 0]
            rop_name = "{} ,  {}".format(rop_name, rop2)
    return rop_name,areaName

def mst_ermitteln(region):
    success= False
    filesinDir = os.listdir(path=os.curdir)
    str_match = list(filter(lambda x: 'bfo-engine_milestones-date.csv' in x, filesinDir))
    file=str_match[0]
    if file=="": return success

    MST= pd.read_csv(file, skipinitialspace=True, sep=';', encoding='utf-8',dtype={'fk_object_id':float})
    MST_TASK = MST
    MST= MST[((MST['milestone_type']=="SERVICE_QUALIFICATION_DATE_TILL") & (MST['type'] == "BUILDING_ORDER"))]
    MST_TASK = MST_TASK[((MST_TASK['milestone_type'] == "NE3_CONSTRUCTION") | (MST_TASK['milestone_type'] == "SITE_SURVEY") | (MST_TASK['milestone_type'] == "NE4_CONSTRUCTION")
                         | (MST_TASK['milestone_type'] == "DPU_CONSTRUCTION")) & (MST_TASK['type'] == "BUILDING_ORDER_SUBTASK")]

    MST.rename(columns={'fk_object_id': 'building_order_id'}, inplace=True)
    MST.set_index('building_order_id',inplace=True)
    MST_TASK.rename(columns={'fk_object_id': 'taskid'}, inplace=True)
    MST_TASK.set_index('taskid', inplace=True)

    FileName = bulk_rep_name(region)
    BFO_PROJ = pd.read_csv(FileName, skipinitialspace=True, delimiter=';', encoding='utf-8')
    BFO_PROJ.astype({'FOL_ID': 'int64'}).dtypes


    BFO_PROJ.insert(17, 'Plandatum Auskundung', "")
    BFO_PROJ.insert(18, 'Umgesetzt Auskundung', "")
    BFO_PROJ.insert(19, 'Plandatum GF AP', "")
    BFO_PROJ.insert(20, 'Umgesetzt GF AP', "")
    BFO_PROJ.insert(21, 'Plandatum GF TA', "")
    BFO_PROJ.insert(22, 'Umgesetzt GF TA', "")
    BFO_PROJ.insert(23, 'Plandatum DPU', "")
    BFO_PROJ.insert(24, 'Umgesetzt DPU', "")

    for i, row in BFO_PROJ.iterrows():
        bo_id = row['Bulk Order ID']
        ProdTaskID= json.loads(row['ProdTaskDict'])
        ONEBOX_ID= ProdTaskID['ONEBOX']
        EXPLORATION_ID=ProdTaskID['EXPLORATION']
        GFTA_ID=ProdTaskID['GFTA']
        DPU_ID=ProdTaskID['DPU']

        if ONEBOX_ID >= 0:
            try:
                ONEBOX_MST=MST_TASK.loc[ONEBOX_ID]
                BFO_PROJ.loc[i, 'Plandatum GF AP']= datum_ermitteln(ONEBOX_MST,'PLANNED')
                BFO_PROJ.loc[i, 'Umgesetzt GF AP']= datum_ermitteln(ONEBOX_MST,'REACHED')
            except Exception as e:
                pass
        if EXPLORATION_ID >= 0:
            try:
                EXPL_MST=MST_TASK.loc[EXPLORATION_ID]
                BFO_PROJ.loc[i, 'Plandatum Auskundung']= datum_ermitteln(EXPL_MST,'PLANNED')
                BFO_PROJ.loc[i, 'Umgesetzt Auskundung']= datum_ermitteln(EXPL_MST,'REACHED')
            except Exception as e:
                pass
        if GFTA_ID >= 0:
            try:
                GFTA_MST=MST_TASK.loc[GFTA_ID]
                BFO_PROJ.loc[i, 'Plandatum GF TA']= datum_ermitteln(GFTA_MST,'PLANNED')
                BFO_PROJ.loc[i, 'Umgesetzt GF TA']= datum_ermitteln(GFTA_MST,'REACHED')
            except Exception as e:
                pass
        if DPU_ID >= 0:
            try:
                GFTA_MST=MST_TASK.loc[DPU_ID]
                BFO_PROJ.loc[i, 'Plandatum DPU']= datum_ermitteln(GFTA_MST,'PLANNED')
                BFO_PROJ.loc[i, 'Umgesetzt DPU']= datum_ermitteln(GFTA_MST,'REACHED')
            except Exception as e:
                pass
        try:
            # Holle nun fuer das Indexierte Bulk Projekt alle Positionen
            MST_ZEILE = MST.loc[bo_id]
            MEILENSTEIN= MST_ZEILE['milestone']
            BFO_PROJ.loc[i,'zu bauen bis'] = MEILENSTEIN[0:10]
        except Exception as e:
            pass
    BFO_PROJ.to_csv(FileName, index=False, sep=';', encoding='utf-8')
    success=True
    return success

def datum_ermitteln(task_mst,what):
    dat = ''
    # Es koennte sein das ein Series Objekt aus dem vorherigem Try entstanden ist
    if in_method(str(type(task_mst)), "Series"):
        # Aufwaendige Transformation des Series wo Objekt in den Dataframe
        col = list(task_mst.index)
        val = list(task_mst.values)
        werte = dict(zip(col, val))
        task_mst = pd.DataFrame(columns=list(werte.keys()))
        for k, v in werte.items():
            task_mst.at[0, k] = v

    if what == "PLANNED":
        cat = task_mst['category'].iloc[0]
        if cat=="PLANNED":
            dat= task_mst['milestone_end'].iloc[0]
        else:
            dat = task_mst['milestone_end'].iloc[1]
    else:
        try:
            cat= task_mst['category'].iloc[1]
            if cat == "REACHED":
                dat = task_mst['milestone_end'].iloc[1]
            else:
                dat = task_mst['milestone_end'].iloc[0]
        except:
            return dat
            print('Kein Reached Dat')
    return dat[0:10]

def nvt_ermitteln(region):
    fields = ['id','status','am_vvm_area_number_fk', 'asb', 'nvt_name','onkz','installation_status','we','ge','zb','internal_expansion_decision_date']
    FOL = pd.read_csv("area_fol.csv", skipinitialspace=True, delimiter=';', dtype={'asb': str, 'onkz': str},
                      encoding='utf-8', usecols=fields)
    FOL.set_index('id',inplace=True)
    FileName = bulk_rep_name(region)
    BFO_PROJ = pd.read_csv(FileName, skipinitialspace=True, delimiter=';', encoding='utf-8')
    BFO_PROJ.astype({'FOL_ID': 'int64'}).dtypes
    BFO_PROJ.set_index('FOL_ID',inplace=True,drop=False)
    BFO_PROJ.insert(4, 'Area Status', "")
    BFO_PROJ.insert(5, 'Ausbauentscheidung Gebiet', "vorhanden")
    BFO_PROJ.insert(12, 'Anzahl WE GE ZB',0)

    for row in BFO_PROJ.itertuples():
        folid=getattr(row, 'Index')
        try:
            # Holle nun fuer das Indexierte Bulk Projekt alle Positionen
            FOL_ZEILE= FOL.loc[folid]
            BFO_PROJ.loc[folid, 'NVt-Bezeichner'] = "{}/{}".format(FOL_ZEILE['onkz'], FOL_ZEILE['nvt_name'])
            WE= int(FOL_ZEILE['we'])
            GE = int(FOL_ZEILE['ge'])
            ZB = int(FOL_ZEILE['zb'])
            if pd.isna(FOL_ZEILE['internal_expansion_decision_date']):
                BFO_PROJ.loc[folid, 'Ausbauentscheidung Gebiet'] = " nicht vorhanden"
            BFO_PROJ.loc[folid, 'Anzahl WE GE ZB'] = int(WE + GE + ZB)
            BFO_PROJ.loc[folid, 'Ausbaustatus'] = FOL_ZEILE['installation_status']
            id = str(folid)
            id = id.replace('.0', "")
            BFO_PROJ.loc[folid, 'FOL_ID'] = id
            BFO_PROJ.loc[folid, 'Area Status'] = FOL_ZEILE['status']
        except:
            continue

    BFO_PROJ.to_csv(FileName, index=False, sep=';', encoding='utf-8')
    return BFO_PROJ

def adressen_ermitteln(region):
    FileName = bulk_rep_name(region)
    BFO_PROJ = pd.read_csv(FileName, skipinitialspace=True, delimiter=';', encoding='utf-8') #,dtype={'FOL_ID':str})
    BFO_PROJ.astype({'FOL_ID': 'int64'}).dtypes
    BFO_PROJ.set_index('KLS-ID',inplace=True, drop=False)

    ADRESSEN = pd.read_csv("address.csv", skipinitialspace=True, delimiter=',',dtype={'zip_code':str,'house_number':str,'house_number_supplement':str},encoding='utf-8')
    ADRESSEN.set_index('kls_id',inplace=True)
    BFO_PROJ.insert(8, 'PLZ', "")
    BFO_PROJ.insert(9, 'Ort', "")
    BFO_PROJ.insert(10, 'Strasse', "")
    BFO_PROJ.insert(11, 'Hausnummer', "")
    BFO_PROJ.insert(12, 'Hausnummer Zusatz', "")
    for row in BFO_PROJ.itertuples():
        kls_id=getattr(row, 'Index')
        try:
            ADR_ZEILE = ADRESSEN.loc[kls_id]
            BFO_PROJ.loc[kls_id, 'PLZ']= ADR_ZEILE['zip_code']
            BFO_PROJ.loc[kls_id, 'Ort'] = ADR_ZEILE['municipality_name']
            BFO_PROJ.loc[kls_id, 'Strasse'] = ADR_ZEILE['street_name']
            BFO_PROJ.loc[kls_id, 'Hausnummer'] = ADR_ZEILE['house_number']
            BFO_PROJ.loc[kls_id, 'Hausnummer Zusatz'] = ADR_ZEILE['house_number_supplement']

        except Exception as e:
            pass
    BFO_PROJ.to_csv(FileName, index=False, sep=';', encoding='utf-8')

def projekte(region):
    FileName = bulk_rep_name(region)
    BFO_PROJ = pd.read_csv(FileName, skipinitialspace=True, delimiter=';', encoding='utf-8', dtype=str)
    BFO_PROJ.astype({'FOL_ID': 'int64'}).dtypes
    #BFO_PROJ.set_index('KLS-ID',inplace=True, drop=False)

    # nun Projekte dazu
    filesinDir = os.listdir(path=os.curdir)
    str_match = list(filter(lambda x: 'program-mgmt_pp_supplier_project.csv' in x, filesinDir))
    if str_match:
        file = str_match[0]
    else:
        return

    PROJ = pd.read_csv(file, skipinitialspace=True, sep=';', encoding='utf-8')
    PROJ.set_index('bid', inplace=True)

    BFO_PROJ.insert(16, 'Status NE3 Projekt', "")
    BFO_PROJ.insert(17, 'Status NE4 Projekt', "")

    for i, row in BFO_PROJ.iterrows():
        projNe3=row['projNe3']
        if not pd.isna(projNe3):
            projNe3 = str(projNe3)
            projNe3 = projNe3.replace('.0', "")
            projNe3=int(projNe3)

        projNe4 = row['projNe4']
        if not pd.isna(projNe4):
            projNe4 = str(projNe4)
            projNe4 = projNe4.replace('.0', "")
            projNe4 = int(projNe4)
        try:
            PROJ_ZEILE_NE3 = PROJ.loc[projNe3]
            BFO_PROJ.loc[i, 'Status NE3 Projekt']=PROJ_ZEILE_NE3['phase']
        except Exception as e:
            pass
        try:
            PROJ_ZEILE_NE4 = PROJ.loc[projNe4]
            BFO_PROJ.loc[i, 'Status NE4 Projekt']=PROJ_ZEILE_NE4['phase']
        except Exception as e:
            pass
    BFO_PROJ.to_csv(FileName, index=False, sep=';', encoding='utf-8')

def ncso_order(region):
    FileName = bulk_rep_name(region)
    BFO_PROJ = pd.read_csv(FileName, skipinitialspace=True, delimiter=';', encoding='utf-8', dtype=str)
    #BFO_PROJ.astype({'FOL_ID': 'int64'}).dtypes
    BFO_PROJ.insert(6, 'Anz. Kundenorder Appartment', 0)
    BFO_PROJ.insert(7, 'Anz. Kundenorder Building', 0)
    BFO_PROJ.insert(8, 'NCSO ProductionLine richtig', "ja")
    BFO_PROJ.insert(43, 'ExterneOrders', "")

    filesinDir = os.listdir(path=os.curdir)
    str_match = list(filter(lambda x: 'fio-core_nso.csv' in x, filesinDir))
    if str_match:
        file = str_match[0]
    else:
        return

    NCSO = pd.read_csv(file, skipinitialspace=True, sep=';', encoding='utf-8')
    NCSO.columns = ["id", "status", "state_reason","prodLine", "extId","FOL_ID","service_type"]
    NCSO.astype({'FOL_ID': 'int64'}).dtypes
    # these NCSO States are not relevant for investigation
    NCSO = NCSO[(NCSO['status'] != "cancelled") & (NCSO['status'] != "completed")]
    NCSO.set_index('FOL_ID', inplace=True,drop=False)

    for i, row in BFO_PROJ.iterrows():
        try:
            FOLID= int(row['FOL_ID'])
            NCSO_FOL= NCSO.loc[FOLID]
            if in_method(str(type(NCSO_FOL)), "Series"):
                # Aufwaendige Transformation des Series wo Objekt in den Dataframe
                col = list(NCSO_FOL.index)
                val = list(NCSO_FOL.values)
                werte = dict(zip(col, val))
                NCSO_FOL = pd.DataFrame(columns=list(werte.keys()))
                for k, v in werte.items():
                    NCSO_FOL.at[0, k] = v
            counter_APP=0
            counter_BUILD=0
            ExternalAuftragsnummer=[]
            prodLineRichtig = "ja"
            for ind, NCSO_ROW in NCSO_FOL.iterrows():
                if NCSO_ROW['service_type']=="fiberAccessApartment":
                    counter_APP+=1
                elif NCSO_ROW['service_type']=="fiberAccessBuilding":
                    counter_BUILD+=1
                extID=NCSO_ROW['extId']
                #
                if NCSO_ROW['prodLine'] !="BuildingOrder" and NCSO_ROW['service_type'] =="fiberAccessApartment" \
                        and row['Ausbauart'] !="Ha Only" and "WOWI" not in extID and NCSO_ROW['status']!="pending":
                    prodLineRichtig= "nein"
                ExternalAuftragsnummer.append(extID)

            BFO_PROJ.loc[i, 'Anz. Kundenorder Appartment'] = counter_APP
            BFO_PROJ.loc[i, 'Anz. Kundenorder Building'] = counter_BUILD
            BFO_PROJ.loc[i, 'NCSO ProductionLine richtig'] = prodLineRichtig
            extAuftrEintrag=str(ExternalAuftragsnummer)
            extAuftrEintrag= extAuftrEintrag.replace("[","").replace("]","").replace("'","")
            BFO_PROJ.loc[i, 'ExterneOrders'] = str(extAuftrEintrag)
        except Exception as e:
            continue

    del BFO_PROJ["projNe3"]
    del BFO_PROJ["projNe4"]
    BFO_PROJ.to_csv(FileName, index=False, sep=';', encoding='utf-8')

def bundesweiter_Report():
    FileList= []

    for key in ["O","N","W","S","SW"]:
        FileName= bulk_rep_name(key)
        FileList.append(FileName)
    i=1
    for key in FileList:
        BFO_DATAFRAME= pd.read_csv(key, skipinitialspace=True, delimiter=';', encoding='ansi',dtype=str)
        if i== 1:
            BFO_BUND=BFO_DATAFRAME
            i+=1
        else:
            BFO_BUND= pd.concat([BFO_BUND, BFO_DATAFRAME])
    FileName = bulk_rep_name("BUND")
    BFO_BUND.to_csv(FileName, index=False, sep=';', encoding='ansi')

def enum_folder(parent_folder, fn):
    """
    :type parent_folder: Folder
    :type fn: (File)-> None
    """
    parent_folder.expand(["Files", "Folders"]).get().execute_query()
    for file in parent_folder.files:  # type: File
        fn(file)
    for folder in parent_folder.folders:  # type: Folder
        enum_folder(folder, fn)
def print_folder_stat(folder):
    """
    :type folder: Folder
    """
    print(folder.serverRelativeUrl)
    print(folder.time_created)

def print_file(f):
    """
    :type f: File
    """
    #print(f.properties['ServerRelativeUrl'])
    print(f.server_relative_path)

if __name__ == '__main__':
    from sharepoint import SharePoint
    # upload file
    file_url = "/sites/BulkReports/Freigegebene Dokumente/General/Bundesweit/Bundesweiter_Bulk_Rep_2022_KW_32.csv"
    SharePoint().download_files(file_url)




    region = ''

    while region != 'Ende':
        region = input(
            "Fuer welche Region moechtest Du den Bulk Report ?\nOst=> O"
            "\nWest => W\nNord => N\nSued => S.\nSuedWest => SW\nGF+ => GF+) :\nBundesweit => BUND :\nneue ADM => ADM : \nEingabe:")
        if region =="ADM":
            neueADMDaten()
            continue
        if region in ["O","N","W","S","SW","GF+"]:
            start = time.time()

            PARTY= partyLesen()
            if PARTY.empty:
                print ("Fehler bei Party Ermittlung. Kein File vorhanden")
                exit()

            #nicht nur party PTI sondern auch die Giga Gebietes von denen. Index ist partyID
            ROP_LOK= ROP(PARTY,region)
            if ROP_LOK.empty:
                print("Fehler beim Lesen der ROP Daten. Kein File vorhanden.")
                exit()
            #im Ergebnis stehen nur die Positionen BFO der PTI OST Gebiete
            BFO = allBFO_read(ROP_LOK)
            if BFO.empty:
                print("Fehler beim Lesen der BFO Daten. Kein File vorhanden")
                exit()

            BFO_PROC= bfo_projecte(BFO,region)
            BUILDING_ORD= bfo_liste(BFO, BFO_PROC,PARTY,ROP_LOK,region)
            print('Nun NVT ermitteln')
            nvt_ermitteln(region)

            print('Nun Meilenstein ermitteln')
            if not mst_ermitteln(region):
                print ("Fehler bei Meilensteine")
                exit()
            print('Nun Adressen ermitteln')
            adressen_ermitteln(region)

            print('Nun Projekte')
            projekte(region)

            print('Nun NCSO')
            ncso_order(region)
            ende = time.time()
            print("execution time {:5.3f}s".format(ende - start))
        elif region=="BUND":
            bundesweiter_Report()
        else:
            print("keine Funktion fuer Dich dabei")
            region = 'Ende'