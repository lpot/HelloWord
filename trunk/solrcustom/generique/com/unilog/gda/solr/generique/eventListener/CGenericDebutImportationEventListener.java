package com.unilog.gda.solr.generique.eventListener;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.handler.dataimport.CGenericJdbcDataSource;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.EventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * ***************************************************************<br>
 * <b>iGDA - Projet SolrComponents</b><br>
 * <b>TYPE</b> :  CGenericDebutImportationEventListener<br>
 * <b>NOM</b> : CGenericDebutImportationEventListener.java<br>
 * <b>SUJET</b> : <br>
 * <b>COMMENTAIRE</b> : <br>
 * **************************************************************
 * 
 * @author ALUG
 * @version $Revision: 1.0 $ $Date: 9 mai 11 16:48:16 $
 */
public class CGenericDebutImportationEventListener implements EventListener{

    /** logger */
    private static final Logger LOG = LoggerFactory.getLogger(CGenericDebutImportationEventListener.class);
    private Double TIMEOUT=60*10.0;//10 minutes
    
    /**
     * @param phtArg0
     * @see org.apache.solr.handler.dataimport.EventListener#onEvent(org.apache.solr.handler.dataimport.Context)
     */
    public void onEvent(Context poContext) {
        LOG.info("============>CGenericDebutImportationEventListener DEBUT<==================\n");
        LOG.info("PARAMS URLS : " + poContext.getRequestParameters().toString());
        LOG.info("============>CGenericDebutImportationEventListener FIN<==================\n");
         
        CGenericJdbcDataSource oDataSource1 = (CGenericJdbcDataSource) poContext.getDataSource("datasource1");
        CGenericJdbcDataSource oDataSource2 = (CGenericJdbcDataSource) poContext.getDataSource("datasource2");
        CGenericJdbcDataSource oDataSource3 = (CGenericJdbcDataSource) poContext.getDataSource("datasource3");
        CGenericJdbcDataSource oDataSource4 = (CGenericJdbcDataSource) poContext.getDataSource("datasource4");
        CGenericJdbcDataSource oDataSource5 = (CGenericJdbcDataSource) poContext.getDataSource("datasource5");
        if(poContext.getRequestParameters().containsKey("indexer")) {
            poContext.getRequestParameters().remove("indexer");
        }
        
                 //récupération du nombre de lignes à traiter dans la table d'indexation
                 Iterator<Map<String, Object>> oRes2 = oDataSource2.getData("SELECT count(*) nbLignes FROM WK_INDEXSOLR WHERE WK_INDEXSOLR_STATUT>0");
                 BigDecimal oNbLignes2 = new BigDecimal(-1);
                 
                 if(oRes2.hasNext()) {
                     oNbLignes2 = (BigDecimal) oRes2.next().get("NBLIGNES");
                 }
                 oDataSource2.close();
                 
                 //si il y a une ligne à traiter
                 if(oNbLignes2.intValue() != 0) {
                     LOG.info("###################### {} LIGNE(S) A INDEXER #########################",oNbLignes2);
                     poContext.getRequestParameters().put("indexer", "true");
                     
                     //on met en erreur les lignes trop longues à être indexées
                     int iNbUpdated = oDataSource5.getDataUpdate("UPDATE WK_INDEXSOLR SET WK_INDEXSOLR_STATUT=-1 WHERE (sysdate - WK_INDEXSOLR_DATE_START)*24*3600 >"+TIMEOUT+" AND WK_INDEXSOLR_STATUT>0");
                     oDataSource5.close();
                     if(iNbUpdated >0) {
                         poContext.getRequestParameters().put("indexer", "false");
                     }
                     
                     
                 }else{
                     LOG.info("###################### AUCUNE LIGNE EN COURS D'INDEXATION #########################");
                     //sinon on incrémente le statut de la première ligne retournée à 1
                     
                     //pour cela on récupère le code de la première ligne
                     Iterator<Map<String, Object>> oRes3 = oDataSource3.getData("SELECT WK_INDEXSOLR_CODE FROM WK_INDEXSOLR WHERE WK_INDEXSOLR_STATUT=0");
                     String sCode="---";
                     
                     //si il y a une ligne dans la table alors ont met à jour son statut
                     if(oRes3.hasNext()) {
                         sCode = (String) oRes3.next().get("WK_INDEXSOLR_CODE");
                         oDataSource3.close();
                   
                         oDataSource1.getDataUpdate("UPDATE WK_INDEXSOLR SET WK_INDEXSOLR_DATE_START=sysdate WHERE WK_INDEXSOLR_CODE='"+sCode+"'");
                         oDataSource1.close();
                         
                         //et on met le staut de cette ligne à 1
                         oDataSource4.getDataUpdate("UPDATE WK_INDEXSOLR SET WK_INDEXSOLR_STATUT=1 WHERE WK_INDEXSOLR_CODE='"+sCode+"'");
                         oDataSource4.close();
                         
                     }else{
                         //sinon il n'y a aucune indexation à effecter
                         LOG.info("###################### AUCUNE LIGNE A INDEXER #########################");
                     }
                     //on indique qu'il n'y aura pas à indexer pour cet import ci
                     poContext.getRequestParameters().put("indexer", "false");
                  }
    }
}
