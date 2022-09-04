def BO_Task_Analyse(BO_Task, BO_State,technik,bulk_state) : #>ReturnListe
    BO_Task.sort_values('subtask_order_id',ascending=True,inplace = True)
    GigaTA_DICT = {"GFTA_GEBAUT": 0, "GFTA_NOCH_NICHT_GEBAUT": 0,
                   "GFTA_IN_BAU": 0}

    if bulk_state=="CREATED":
        return ["Bulk Projekt freigeben", GigaTA_DICT]
    if BO_State == "CREATED":
        return ["Bitte Ticket an die IT. Dieser Zustand dürfte nicht auftreten", GigaTA_DICT]

    if BO_State =="ERROR":
        for i, task in BO_Task.iterrows():
            if task["subtask_type"] =="START_PON_PLANNING" and task["subtask_state"] =="ERROR":
                error_content= PON_ERROR_CONTENT_ANALYSE(task["error_content"])
                return ["PON Planungsfehler beseitigen",GigaTA_DICT,error_content]
            elif task["subtask_type"] =="NE3_REQUEST" and task["subtask_state"] =="ERROR":
                return ["Ermittlung NE3 Supplier in Bulk via Retry erneut ermitteln.",GigaTA_DICT]
            elif task["subtask_type"] =="NE4_REQUEST" and task["subtask_state"] =="ERROR":
                return ["Ermittlung NE4 Supplier in Bulk via Retry erneut ermitteln.",GigaTA_DICT]
            elif task["subtask_type"] =="CANCEL_PON_PLANNING" and task["subtask_state"] =="ERROR":
                return ["PON Planung zur Loaktion im PON Inventory loeschen und dann in Bulk die Loaktion via Skip bearbeiten",GigaTA_DICT]
            elif task["subtask_type"] =="NSO_STATUS_CHECK" and task["subtask_state"] =="ERROR":
                return ["Pruefung der IBT Order noch in Ausfuehrung. Das ist ein Fehler der via Nimbus Ticket mit Angabe der KLS ID zum Problemmanagement T-Magic zu adressieren ist.",GigaTA_DICT]

            elif task["subtask_type"] =="UPDATE_TECHNOLOGY_ON_FOL" and task["subtask_state"] =="ERROR":
                return ["Der Technikwechsel zu FFTB hat nicht funktioniert. Das ist ein Fehler der via Nimbus Ticket mit Angabe der KLS ID zum Problemmanagement T-Magic zu adressieren ist.",GigaTA_DICT]

            elif task["subtask_type"] =="BUILDING_ORDER_CANCELATION" and task["subtask_state"] =="ERROR":
                return ["Das Loeschen der PON Planung beim Cancel der Bulk Order hat nicht funktioniert. Bitte PON Planung loeschen fuer diese Lokation und "
                        "dann im Bulk fuer diese KLS die Position mit Skip behandeln.",GigaTA_DICT]
        return ["noch nicht ermittelbar.",GigaTA_DICT]
    elif BO_State =="APPROVED":
        ponPlanungSkipped=False
        for i, task in BO_Task.iterrows():
            # this Series is important
            if task["subtask_type"] == "START_PON_PLANNING" and task["subtask_state"] == "SKIPPED":
                ponPlanungSkipped=True
                continue
            if task["subtask_type"] =="NE3_REQUEST" and task["subtask_state"] =="IN_PROGRESS":
                return ["Manuelle Disposition zur Bestimmung des NE3 Supplier durchführen", GigaTA_DICT]
            if task["subtask_type"] =="NE4_REQUEST" and task["subtask_state"] =="IN_PROGRESS":
                return ["Manuelle Disposition zur Bestimmung des NE4 Supplier durchführen", GigaTA_DICT]

            if task["subtask_type"] == "EXPLORATION" and task["subtask_state"] == "CREATED":
                if ponPlanungSkipped:
                    return ["Termin Auskundung planen. Achtung PON Planung wurde uebersprungen",GigaTA_DICT]
                else:
                    return ["Termin Auskundung planen.",GigaTA_DICT]
            elif task["subtask_type"] == "ONEBOX" and task["subtask_state"] == "CREATED":
                if ponPlanungSkipped:
                    return ["Termin Bau GF AP planen. Achtung PON Planung wurde uebersprungen",GigaTA_DICT]
                else:
                    return ["Termin Bau GF AP planen",GigaTA_DICT]
            elif "GFTA" in task["subtask_type"] and task["subtask_state"] == "CREATED":
                return ["Bau Giga Dosen Termin planen",GigaTA_DICT]
            elif task["subtask_type"] =="DPU_PLANNING_DEMAND" and task["subtask_state"] == "CREATED":
                return ["DPU Planung durchfuehren",GigaTA_DICT]
            elif task["subtask_type"] =="DPU" and task["subtask_state"] == "CREATED":
                return ["Bau DPU Termin planen",GigaTA_DICT]
        return "noch nicht ermittelbar."
    elif BO_State=="WAITING":
        for i, task in BO_Task.iterrows():

            if task["subtask_type"] == "NSO_STATUS_CHECK" and task["subtask_state"] == "IN_PROGRESS":
                return ["Error: Bitte Ticket an IT. Wartet auf Check IBT Orders",GigaTA_DICT]
            elif task["subtask_type"] == "PRV_AND_NBG_DATE" and task["subtask_state"] == "IN_PROGRESS":
                return ["Wartet auf die Erfassung der Eigenleistung durch Bauherr",GigaTA_DICT]

            # das duerfte es nicht geben
            #elif task["subtask_type"] == "DPU_PLANNING_DEMAND" and task["subtask_state"] == "WAITING":
            #    return ["Wartet auf das Planungsergebnis der DPU Planung",GigaTA_DICT]

        return "return Fall noch nicht abgefangen"
    elif BO_State=="IN_PROGRESS" or BO_State=="COMPLETED":
        ponPlanungSkipped = False
        AUSKUNDUNG = False
        GFAP=False
        DPU=False
        GFTA_TASK_ANGELEGT = 0
        GFTA_GEBAUT = 0
        GFTA_NOCH_NICHT_GEBAUT = 0
        GFTA_IN_BAU=0

        for i, task in BO_Task.iterrows():
            # this Series is important
            if task["subtask_type"] == "START_PON_PLANNING" and task["subtask_state"] == "SKIPPED":
                ponPlanungSkipped=True
                continue

            if task["subtask_type"] == "PRV_AND_NBG_DATE" and task["subtask_state"] == "IN_PROGRESS":
                return ["Warten auf die Erfassung der Eigenleistung vom Bauherren",GigaTA_DICT]

            elif task["subtask_type"] == "EXPLORATION" and task["subtask_state"] == "CREATED":
                if ponPlanungSkipped:
                    return ["Termin Auskundung planen. Achtung PON Planung wurde uebersprungen",GigaTA_DICT]
                else:
                    return ["Termin Auskundung planen",GigaTA_DICT]
            elif task["subtask_type"] == "EXPLORATION" and task["subtask_state"] == "APPROVED":
                if ponPlanungSkipped:
                    return ["Auskundung wurde geplant, nun durchfuehren. Achtung PON Planung wurde uebersprungen",GigaTA_DICT]
                else:
                    return ["Auskundung wurde geplant, nun durchfuehren.",GigaTA_DICT]
            elif task["subtask_type"] == "EXPLORATION" and task["subtask_state"] == "COMPLETED":
                AUSKUNDUNG=True

            elif task["subtask_type"] == "DPU_PLANNING_DEMAND" and (task["subtask_state"] == "CREATED" or task["subtask_state"] == "WAITING"):
                return ["Wartet auf das Planungsergebnis der DPU Planung", GigaTA_DICT]

            elif task["subtask_type"] == "ONEBOX" and task["subtask_state"] == "CREATED":
                if ponPlanungSkipped:
                    return ["Bau GF AP muss noch geplant werden. Achtung PON Planung wurde uebersprungen",GigaTA_DICT]
                else:
                    return ["Bau GF AP muss noch geplant werden",GigaTA_DICT]
            elif task["subtask_type"] == "ONEBOX" and task["subtask_state"] == "APPROVED":
                if ponPlanungSkipped:
                    return ["Bau GF AP wurde geplant, muss nun gebaut werden. Achtung PON Planung wurde uebersprungen",GigaTA_DICT]
                else:
                    return ["Bau GF AP wurde geplant, muss nun gebaut werden.",GigaTA_DICT]
            elif task["subtask_type"] == "ONEBOX" and task["subtask_state"] == "IN_PROGRESS":
                if ponPlanungSkipped:
                        return ["Bau GF AP in Ausfuehrung. Achtung PON Planung wurde uebersprungen",GigaTA_DICT]
                else:
                    return ["Bau GF AP in Ausfuehrung",GigaTA_DICT]
            elif task["subtask_type"] == "ONEBOX" and task["subtask_state"] == "COMPLETED":
                GFAP=True

            elif task["subtask_type"] == "DPU" and task["subtask_state"] == "CREATED":
                return ["Termin Bau DPU u. Dosen muss geplant werden", GigaTA_DICT]
            elif task["subtask_type"] == "DPU" and task["subtask_state"] == "APPROVED":
                #pruefe nun nicht ob es auch Termine fuer Dosen gibt. Das sieht man im Report bei den MST fuer Dosen
                return ["Bau DPU wurde geplant, muss nun gebaut werden", GigaTA_DICT]
            elif task["subtask_type"] == "DPU" and task["subtask_state"] == "IN_PROGRESS":
                return ["Bau DPU in Ausfuehrung, nicht abgeschlossen. Warten auf Fertigstellung", GigaTA_DICT]
            elif task["subtask_type"] == "DPU" and task["subtask_state"] == "COMPLETED":
                #der Kenner spielt noch keine Rolle
                DPU=True

            elif "GFTA" in task["subtask_type"]  and task["subtask_state"] == "CREATED":
                GFTA_TASK_ANGELEGT+=1
            elif "GFTA" in task["subtask_type"]  and task["subtask_state"] == "COMPLETED":
                GFTA_GEBAUT+=1
            elif "GFTA" in task["subtask_type"]  and task["subtask_state"] == "APPROVED":
                GFTA_NOCH_NICHT_GEBAUT +=1
            elif "GFTA" in task["subtask_type"]  and task["subtask_state"] == "IN_PROGRESS":
                GFTA_IN_BAU +=1

        GigaTA_DICT ={"GFTA_GEBAUT":GFTA_GEBAUT,"GFTA_NOCH_NICHT_GEBAUT":GFTA_NOCH_NICHT_GEBAUT,"GFTA_IN_BAU":GFTA_IN_BAU}


        if GFTA_GEBAUT > 0 and GFTA_NOCH_NICHT_GEBAUT > 0 and GFTA_IN_BAU==0:
            return  ["Restliche Dosen bauen",GigaTA_DICT]
        elif GFTA_GEBAUT > 0 and GFTA_NOCH_NICHT_GEBAUT > 0 and GFTA_IN_BAU>0:
            return ["Achtung: Bau Prozess Dose gestartet aber fehlerhaft. Restliche Dosen bauen",GigaTA_DICT]
        elif GFTA_GEBAUT >0 and GFTA_IN_BAU == 0 and GFTA_NOCH_NICHT_GEBAUT==0:
            return ["keiner mehr da alle Dosen gebaut wurden",GigaTA_DICT]
        elif GFTA_GEBAUT >0 and GFTA_IN_BAU > 0 and GFTA_NOCH_NICHT_GEBAUT==0:
            return ["Achtung: Bau Prozess Dose gestartet aber einige fehlerhaft",GigaTA_DICT]
        elif GFTA_GEBAUT == 0 and GFTA_IN_BAU == 0 and GFTA_NOCH_NICHT_GEBAUT>0:
            return ["Alle Dosen bauen",GigaTA_DICT]
        elif GFTA_TASK_ANGELEGT > 0 and GFAP == True and AUSKUNDUNG == True:
            return ["Alle Dosen muessen noch gebaut werden. Noch kein Termin dafuer vorhanden",GigaTA_DICT]
        elif GFTA_GEBAUT == 0 and GFTA_IN_BAU == 0 and GFTA_NOCH_NICHT_GEBAUT==0 and BO_State=="COMPLETED":
            #Mueste HA Only sein
            return ["keiner",GigaTA_DICT]
        else:
            return ["Fall noch nicht abgefangen",GigaTA_DICT]

def PON_ERROR_CONTENT_ANALYSE(error_content):
    error_content= error_content.strip('"\\"')
    error_content = error_content.strip('"')

    if "HK fibers" in error_content or "hkFibers":
        return "Zu wenige HK Kabel"
    elif "BadGateway" in error_content:
        return "IT Fehler in API Plattform. Wird durch IT behoben"
    elif "Gateway Timeout" in error_content:
        return "IT Fehler in API Plattform. Bitte retry Button nutzen"
    elif "400 (Bad Request). An error while parsing" in error_content:
        return "IT Fehler im Bulk Modul PonPlanning. Bitte retry Button nutzen"
    elif "(line:37)" in error_content:
        return "IT Fehler. Bitte retry ausführen."
    elif "" in error_content:
        return "Kein Inhalt im Fehlertext. Bitte retry ausführen."
    else:
        return "noch nicht interpretierbar"


