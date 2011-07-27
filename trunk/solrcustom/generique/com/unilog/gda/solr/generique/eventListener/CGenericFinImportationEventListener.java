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
 * <b>TYPE</b> :  CGenericFinImportationEventListener<br>
 * <b>NOM</b> : CGenericFinImportationEventListener.java<br>
 * <b>SUJET</b> : <br>
 * <b>COMMENTAIRE</b> : <br>
 * **************************************************************
 * 
 * @author ALUG
 * @version $Revision: 1.0 $ $Date: 9 mai 11 16:50:07 $
 */
public class CGenericFinImportationEventListener implements EventListener{
    
    /** Limitation du nombre de lignes ramenées par défaut */
    private static final int MAX_TENTATIVES = 200;
   
    /** logger */
    private static final Logger LOG = LoggerFactory.getLogger(CGenericDebutImportationEventListener.class);
    
    
    /**
     * Méthode mettant à jour la table d’indexation en base de données suite à une indexation solr.
     * @param poContext le contexte
     * @see org.apache.solr.handler.dataimport.EventListener#onEvent(org.apache.solr.handler.dataimport.Context)
     */
    public void onEvent(Context poContext) {
        LOG.info("============>CGenericFinImportationEventListener DEBUT<==================");
        LOG.info("----->PARAMS : \n " +poContext.getRequestParameters().toString());
        Iterator<Map<String, Object>> oDataResults = null;
        
        CGenericJdbcDataSource oDataSource = (CGenericJdbcDataSource) poContext.getDataSource("datasource");
        
        Long oSkipDocCount = (Long) poContext.getStats().get("skipDocCount");
        Long oDocCount = (Long) poContext.getStats().get("docCount");
        Long oDeletedDocCount = (Long) poContext.getStats().get("deletedDocCount");
        
        LOG.info("-----------------------INFORMATIONS CONCERNANT L'IMPORTATION-----------------------------");
        LOG.info("----->skipDocCount : "+oSkipDocCount);
        LOG.info("----->docCount : "+oDocCount);
        LOG.info("----->deletedDocCount : "+oDeletedDocCount);
        LOG.info("-----------------------------------------------------------------------------------------");
            
        if(poContext.getRequestParameters().get("indexer") == "true") {
           //si l'opération s'est bien passée on supprime la ligne dans la table d'indexation
           if(oSkipDocCount == 0 &&
              (oDocCount + oDeletedDocCount == 1) ) {
               LOG.info("---->SUPPRESSION...");
               int iNbModif = oDataSource.getDataUpdate("DELETE FROM WK_INDEXSOLR i WHERE i.WK_INDEXSOLR_statut BETWEEN 1 AND " +MAX_TENTATIVES);
               LOG.info("---->NOMBRE DE LIGNES SUPPRIMEES : "+iNbModif);
            //sinon on incrémente le staut pour signaler une erreur et ainsi pouvoir retntenter l'opération
            }else{
                LOG.error("---->ERREUR D'INDEXATION");
                oDataResults = oDataSource.getData("SELECT WK_INDEXSOLR_STATUT FROM WK_INDEXSOLR i WHERE i.WK_INDEXSOLR_statut BETWEEN 1 AND " +MAX_TENTATIVES);
                CGenericJdbcDataSource oDataSource2 = (CGenericJdbcDataSource) poContext.getDataSource("datasource2");
                while(oDataResults.hasNext()) {
                    Map<String, Object> oCurrentResult = oDataResults.next();
                    BigDecimal oStatut = (BigDecimal) oCurrentResult.get("WK_INDEXSOLR_STATUT");
                    int iStatut = oStatut.intValue();
                    LOG.info("ancien statut : "+iStatut);
                    
                    if(iStatut == MAX_TENTATIVES) {
                        iStatut = -1;
                    }else{
                        iStatut++;
                    }
                    LOG.info("nouveau statut : "+iStatut);
                    
                    oDataSource2.getDataUpdate("UPDATE WK_INDEXSOLR set WK_INDEXSOLR_statut='"+iStatut+"' WHERE WK_INDEXSOLR_statut BETWEEN 1 AND " +MAX_TENTATIVES);
                }
                oDataSource2.close();
            }
        oDataSource.close();
        }else{
            LOG.info("---->AUCUNE LIGNE N'A ETE INDEXEE");
            if(poContext.getRequestParameters().containsKey("indexer")) {
                poContext.getRequestParameters().remove("indexer");
            }
            poContext.getRequestParameters().put("indexer", "true");
        }
        LOG.info("============>CGenericFinImportationEventListener FIN<==================");
    }
}
