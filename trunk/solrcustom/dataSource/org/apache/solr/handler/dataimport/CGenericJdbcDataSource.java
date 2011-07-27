package org.apache.solr.handler.dataimport;

import java.sql.Connection;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * ***************************************************************<br>
 * <b>iGDA - Projet SolrComponents</b><br>
 * <b>TYPE</b> :  CGenericJdbcDataSource<br>
 * <b>NOM</b> : CGenericJdbcDataSource.java<br>
 * <b>SUJET</b> : <br>
 * <b>COMMENTAIRE</b> : <br>
 * **************************************************************
 * 
 * @author ALUG
 * @version $Revision: 1.0 $ $Date: 9 mai 11 16:38:21 $
 */
public class CGenericJdbcDataSource extends JdbcDataSource {
    
    private long lConnLastUsed = 0;

    /** Connection à la base de données */
    private Connection oConn;

    /** logger */
    private static final Logger LOG = LoggerFactory.getLogger(CGenericJdbcDataSource.class);
    
    /** temps avant l'expiration de la connection */
    private static final long CONN_TIME_OUT = 10 * 1000; // 10 seconds
    
    /**
     * Récupère la connection à la base de données.
     * @return la conenction
     * @throws Exception
     */
    private Connection getConnection() throws Exception {
       
        long lCurrTime = System.currentTimeMillis();
        if (lCurrTime - lConnLastUsed > CONN_TIME_OUT) {
          synchronized (this) {
            Connection oTmpConn = factory.call();
            closeConnection();
            lConnLastUsed = System.currentTimeMillis();
            oConn = oTmpConn;
          }

        } else {
          lConnLastUsed = lCurrTime;
        }
        oConn.setAutoCommit(true);
        return oConn;
      }
    

    /**
     * Effectue un update d'un requête sql.
     * @param psQuery la requête sql
     * @return le nombre de lignes update
     */
    public int getDataUpdate(String psQuery) {
        LOG.info("query : " + psQuery);
        Statement pStmt = null;
        int res = 0;
            Connection c;
            try {
                c = getConnection();
                c.setAutoCommit(true);
                pStmt = c.createStatement();
                res = pStmt.executeUpdate(psQuery);
                LOG.info("RES : " + res);
            } catch (Exception oE) {
                oE.printStackTrace();
            }
            return res; 
      }
    
    /**
     * Ferme la connection à la base de données.
     */
    private void closeConnection()  {
        try {
          if (oConn != null) {
            oConn.close();
          }
        } catch (Exception e) {
          LOG.error("Ignoring Error when closing connection", e);
        }
      }
}
