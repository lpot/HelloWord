package com.unilog.gda.solr.specifique.process.gmao;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import org.apache.solr.handler.dataimport.CGenericJdbcDataSource;
import org.apache.solr.handler.dataimport.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unilog.gda.solr.generique.process.CGenericElementProcessor;

/**
 * 
 * ***************************************************************<br>
 * <b>iGDA - Projet SolrComponents</b><br>
 * <b>TYPE</b> :  CGmaoEquipementProcessor<br>
 * <b>NOM</b> : CGmaoEquipementProcessor.java<br>
 * <b>SUJET</b> : <br>
 * <b>COMMENTAIRE</b> : <br>
 * **************************************************************
 * 
 * @author ALUG
 * @version $Revision: 1.0 $ $Date: 9 mai 11 17:05:59 $
 */
public class CGmaoEquipementProcessor extends CGenericElementProcessor{

    /** logger */
    private static final Logger LOG = LoggerFactory.getLogger(CGmaoEquipementProcessor.class);
    
    public String traitement; 
    
    
    
   /**
    * Construit un équipement et rajoute les attributs correspondants
    * @return l'équipement sous forme de map
    * @see org.apache.solr.handler.dataimport.SqlEntityProcessor#nextRow()
    */
    public Map<String, Object> nextRow() {
        LOG.info("===================>CGmaoEquipementProcessor DEBUT<======================");
        
        Map<String, Object> oRes = super.nextRow();
        Map<String, Object> oMap0;
        Map<String, Object> oMap1;
        Map<String, Object> oMap2;
        Map<String, Object> oMap3;
        Map<String, Object> oMapHasart;
        Map<String, Object> oMapHalivr;
        Map<String, Object> oMapHalivrAffect;
        Map<String, Object> oMapProtocoles;
        Map<String, Object> oMapEltpats;
        Map<String, Object> oMapCompteurs;
        Map<String, Object> oMapTaches;
        
        if(oRes != null) {
            LOG.info("--->oRes : "+oRes.toString());
            BigDecimal oCode = (BigDecimal) oRes.get("EQUIPEMENT_CODE");
            Vector<CGenericJdbcDataSource> vDataSource = new Vector<CGenericJdbcDataSource>();
            //dataSource0
            vDataSource.add(0,(CGenericJdbcDataSource) context.getDataSource("dataSource0"));
            //récupération du CDR_LIB associé à l'équipement même
            Iterator<Map<String, Object>> resultSelect0 = 
                vDataSource.get(0).getData("SELECT CDR_LIB FROM CDR C, EQUIPEMENT E " +
                		                   "WHERE "+oCode+" = E.EQUIPEMENT_CODE " +
                		                   "AND E.CDR_CODE = C.CDR_CODE");
            
            if(resultSelect0.hasNext()) {
                oMap0 = resultSelect0.next();
                String sCdrLib = (String) oMap0.get("CDR_LIB");
                oRes.put("CDR_LIB", sCdrLib);
            }
            
            vDataSource.get(0).close();
            
            //maj des attributs
            //dataSource1
            vDataSource.add(1,(CGenericJdbcDataSource) context.getDataSource("dataSource1"));
            Iterator<Map<String, Object>> resultSelect1 = 
            vDataSource.get(1).getData("SELECT P.PARAMATTR_LIB LIB, AE.ATTR_EQUIPEMENT_VALEUR VAL, AE.LIENATTR_CODE LIENATTR_CODE " +
                                       "FROM ATTR_EQUIPEMENT AE, ATTR A, PARAMATTR P " +
                                       "WHERE "+oCode+" = AE.EQUIPEMENT_CODE " +
                                       "AND AE.ATTR_CODE = A.ATTR_CODE " +
                		               "AND A.PARAMATTR_CODE = P.PARAMATTR_CODE");
            
      
            int i = 2;
            //pour chaque attribut on récupère les données que l'on souhaite
            while(resultSelect1.hasNext()) {
                oMap1 = resultSelect1.next();
                //libellé et valeur
                String sAttr_nom = (String) oMap1.get("LIB");
                String sAttr_valeur = (String) oMap1.get("VAL");
                //lien vers la table LIENATTR
                BigDecimal oLIENATTR_CODE = (BigDecimal) oMap1.get("LIENATTR_CODE");
                
                
                LOG.info("att_"+sAttr_nom+" , attr_valeur : "+sAttr_valeur);
                
                // si c'est une donnée grand angle, 
                // on va chercher les informations complémentaires
                if(oLIENATTR_CODE != null) {
                    LOG.info("--------->Attribut Grand Angle");
                    int iLIENATTR_CODE = oLIENATTR_CODE.intValue();
                    LOG.info("LIENATTR_CODE = "+iLIENATTR_CODE);
                    
                        //datasource2
                        vDataSource.add(i,(CGenericJdbcDataSource) context.getDataSource(""+iLIENATTR_CODE));
                        // on récupère ensuite la colonne de la table LIENATTR qui n'est pas null 
                        Iterator<Map<String, Object>> resultSelect2 = 
                            vDataSource.get(i).getData("SELECT SEDOVLIENATTR("+iLIENATTR_CODE+") CODECOL, SEDOVLIENATTR_COLONNES("+iLIENATTR_CODE+") NOMCOL FROM DUAL");
                            i++;
                        if(resultSelect2.hasNext()) {
                            oMap2 = resultSelect2.next();
                            String sCodeCol = (String) oMap2.get("CODECOL");
                            String sNomCol = (String) oMap2.get("NOMCOL");
                            int iCodeCOl = Integer.parseInt(sCodeCol);
                            Iterator<Map<String, Object>> resultSelect3;
                            LOG.info("----->NOMCOL : "+sNomCol);
                            LOG.info("----->CODECOL : "+iCodeCOl);
                            
                            
                          //datasource3
                            vDataSource.add(i,(CGenericJdbcDataSource) context.getDataSource(sNomCol));
                            
                            if(sNomCol.equals("UTILIS_CODE")){
                                resultSelect3 = 
                                    vDataSource.get(i).getData("SELECT UTILIS_IDENT, UTILIS_PRENOM, UTILIS_NOM " +
                                		             "FROM UTILIS " +
                                		             "WHERE UTILIS_CODE = "+iCodeCOl);
                                if(resultSelect3.hasNext()) {
                                    oMap3 = resultSelect3.next();
                                    String sUtilisIdent = (String) oMap3.get("UTILIS_IDENT");
                                    String sUtilisPrenom = (String) oMap3.get("UTILIS_PRENOM");
                                    String sUtilisNom = (String) oMap3.get("UTILIS_NOM");
                                    ArrayList<String> oArrAttributs = new ArrayList<String>();
                                    oArrAttributs.add(sUtilisIdent);
                                    oArrAttributs.add(sUtilisPrenom);
                                    oArrAttributs.add(sUtilisNom);
                                    oRes.put("att_"+sAttr_nom,oArrAttributs);
                                }
                                
                            }else if(sNomCol.equals("HASART_CODE")) {
                                resultSelect3 = 
                                    vDataSource.get(i).getData("SELECT HASART_LIB " +
                                                     "FROM HASART " +
                                                     "WHERE HASART_CODE = "+iCodeCOl);
                                if(resultSelect3.hasNext()) {
                                    oMap3 = resultSelect3.next();
                                    String sHasartLib = (String) oMap3.get("HASART_LIB");
                                    oRes.put("att_"+sAttr_nom,sHasartLib);
                                }
                                
                            }else if(sNomCol.equals("TIERS_CODE")) {
                                resultSelect3 = 
                                    vDataSource.get(i).getData("SELECT TIERS_NOM " +
                                                     "FROM TIERS " +
                                                     "WHERE TIERS_CODE = "+iCodeCOl);
                                if(resultSelect3.hasNext()) {
                                    oMap3 = resultSelect3.next();
                                    String sTiersLib = (String) oMap3.get("TIERS_NOM");
                                    oRes.put("att_"+sAttr_nom,sTiersLib);
                                }
                            }else if(sNomCol.equals("CORRES_CODE")) {
                                resultSelect3 = 
                                    vDataSource.get(i).getData("SELECT CORRES_NOM, CORRES_PRENOM " +
                                                     "FROM CORRES " +
                                                     "WHERE CORRES_CODE = "+iCodeCOl);
                                if(resultSelect3.hasNext()) {
                                    oMap3 = resultSelect3.next();
                                    String sCorresNom= (String) oMap3.get("CORRES_NOM");
                                    String sCorresPrenom= (String) oMap3.get("CORRES_PRENOM");
                                    ArrayList<String> oArrAttributs = new ArrayList<String>();
                                    oArrAttributs.add(sCorresNom);
                                    oArrAttributs.add(sCorresPrenom);
                                    oRes.put("att_"+sAttr_nom,oArrAttributs);
                                }
                            }else if(sNomCol.equals("SECTO_NUM")) {
                                resultSelect3 = 
                                    vDataSource.get(i).getData("SELECT SECTO_CODE, SECTO_DESCRI " +
                                                     "FROM SECTO " +
                                                     "WHERE SECTO_NUM = "+iCodeCOl);
                                if(resultSelect3.hasNext()) {
                                    oMap3 = resultSelect3.next();
                                    String sSectoCode= (String) oMap3.get("SECTO_CODE");
                                    String sSectoDescri= (String) oMap3.get("SECTO_DESCRI");
                                    ArrayList<String> oArrAttributs = new ArrayList<String>();
                                    oArrAttributs.add(sSectoCode);
                                    oArrAttributs.add(sSectoDescri);
                                    oRes.put("att_"+sAttr_nom,oArrAttributs);
                                }
                            }else if(sNomCol.equals("CDR_CODE")) {
                                resultSelect3 = 
                                    vDataSource.get(i).getData("SELECT CDR_LIB " +
                                                     "FROM CDR " +
                                                     "WHERE CDR_CODE = "+iCodeCOl);
                                if(resultSelect3.hasNext()) {
                                    oMap3 = resultSelect3.next();
                                    String sCdrLib= (String) oMap3.get("CDR_LIB");
                                    oRes.put("att_"+sAttr_nom,sCdrLib);
                                }
                            }else if(sNomCol.equals("HALIVR_CODE")) {
                                resultSelect3 = 
                                    vDataSource.get(i).getData("SELECT HALIVR_LIB " +
                                                     "FROM HALIVR " +
                                                     "WHERE HALIVR_CODE = "+iCodeCOl);
                                if(resultSelect3.hasNext()) {
                                    oMap3 = resultSelect3.next();
                                    String sHalivrLib= (String) oMap3.get("HALIVR_LIB");
                                    oRes.put("att_"+sAttr_nom,sHalivrLib);
                                }
                            }else if(sNomCol.equals("POSTCOM_CODE")) {
                                resultSelect3 = 
                                    vDataSource.get(i).getData("SELECT POSTCOM_CODEXT, POSTCOM_DESCRI " +
                                                     "FROM POSTCOM " +
                                                     "WHERE POSTCOM_CODE = "+iCodeCOl);
                                if(resultSelect3.hasNext()) {
                                    oMap3 = resultSelect3.next();
                                    String sPostcomCodext = (String) oMap3.get("POSTCOM_CODEXT");
                                    String sPostcomDescri = (String) oMap3.get("POSTCOM_DESCRI");
                                    ArrayList<String> oArrAttributs = new ArrayList<String>();
                                    oArrAttributs.add(sPostcomCodext);
                                    oArrAttributs.add(sPostcomDescri);
                                    oRes.put("att_"+sAttr_nom,oArrAttributs);
                                }
                            }
                            //fermeture datasource3
                            vDataSource.get(i).close();
                        }
                      //fermeture datasource2
                        vDataSource.get(i-1).close(); 
                        i++;
                 //sinon on ajoute l'attribut avec sa valeur
                }else{
                    LOG.info("----------->Pas un attribut grand angle");
                    oRes.put("att_"+sAttr_nom, sAttr_valeur);
                }
             
                LOG.info("---------\n");
            }
            vDataSource.get(1).close();
            
            
            
            //maj HALIVR
            CGenericJdbcDataSource oDataSourceHalivr = (CGenericJdbcDataSource) context.getDataSource("dataSourceHalivr");
            
            Iterator<Map<String, Object>> resultSelectHalivr = oDataSourceHalivr.getData("SELECT HALIVR_CODEXT, HALIVR_LIB " +
            		                                       "FROM HALIVR H, EQUIPEMENT E " +
            		                                       "WHERE "+oCode+" = E.EQUIPEMENT_CODE " +
            		                                       "AND E.HALIVR_CODE = H.HALIVR_CODE");
            
            if(resultSelectHalivr.hasNext()) {
                oMapHalivr = resultSelectHalivr.next();
                String sHalivrCodext = (String) oMapHalivr.get("HALIVR_CODEXT");
                oRes.put("HALIVR_CODEXT", sHalivrCodext);
                String sHalivrLib = (String) oMapHalivr.get("HALIVR_LIB");
                oRes.put("HALIVR_LIB", sHalivrLib);
            }
            
            oDataSourceHalivr.close();
            
            //maj HALIVR_AFFECT
            CGenericJdbcDataSource oDataSourceHalivrAffect = (CGenericJdbcDataSource) context.getDataSource("dataSourceHalivr");
            
            Iterator<Map<String, Object>> resultSelectHalivrAffect = oDataSourceHalivrAffect.getData("SELECT HALIVR_CODEXT, HALIVR_LIB " +
            		                                       "FROM HALIVR H, EQUIPEMENT E " +
            		                                       "WHERE "+oCode+" = E.EQUIPEMENT_CODE " +
            		                                       "AND E.HALIVR_CODEAFFECT = H.HALIVR_CODE");
            
            if(resultSelectHalivrAffect.hasNext()) {
                oMapHalivrAffect = resultSelectHalivrAffect.next();
                String sHalivrCodext = (String) oMapHalivrAffect.get("HALIVR_CODEXT");
                oRes.put("HALIVR_AFFECT_CODEXT", sHalivrCodext);
                String sHalivrLib = (String) oMapHalivrAffect.get("HALIVR_LIB");
                oRes.put("HALIVR_AFFECT_LIB", sHalivrLib);
            }
            
            oDataSourceHalivrAffect.close();

            //maj HASART
            CGenericJdbcDataSource oDataSourceHasart = (CGenericJdbcDataSource) context.getDataSource("dataSourceHasart");
            
            Iterator<Map<String, Object>> resultSelectHasart = oDataSourceHasart.getData("SELECT HASART_LIB " +
                                                           "FROM HASART H, EQUIPEMENT E " +
                                                           "WHERE "+oCode+" = E.EQUIPEMENT_CODE " +
                                                           "AND E.HASART_CODE = H.HASART_CODE");
            
            if(resultSelectHasart.hasNext()) {
                oMapHasart = resultSelectHasart.next();
                String sHasartLib = (String) oMapHasart.get("HASART_LIB");
                oRes.put("HASART_LIB", sHasartLib);
            }
            
            oDataSourceHasart.close();
            
            //maj protocoles
            CGenericJdbcDataSource oDataSourceProtocoles = (CGenericJdbcDataSource) context.getDataSource("dataSourceProtocoles");
            
            Iterator<Map<String, Object>> resultSelectProtocoles = 
                oDataSourceProtocoles.getData("SELECT P.PROTOCOLE_CODE CODE,P.PROTOCOLE_LIB LIB, P.PROTOCOLE_CODEXT CODEXT " +
                		                      "FROM PROTOCOLE P, EQUIPEMENT_PROTOCOLE EP " +
                		                      "WHERE "+oCode+" = EP.EQUIPEMENT_CODE " +
                		                      "AND EP.PROTOCOLE_CODE = P.PROTOCOLE_CODE");
            
            ArrayList<String> oArrProtocoles = new ArrayList<String>();
            while(resultSelectProtocoles.hasNext()) {
                oMapProtocoles = resultSelectProtocoles.next();
                oArrProtocoles.add((String) oMapProtocoles.get("CODEXT"));
                oArrProtocoles.add((String) oMapProtocoles.get("LIB"));
            }
            oRes.put("protocole", oArrProtocoles);
            oDataSourceProtocoles.close();
            
          //maj eltpat
            CGenericJdbcDataSource oDataSourceEltpats = (CGenericJdbcDataSource) context.getDataSource("dataSourceEltpats");
            
            Iterator<Map<String, Object>> resultSelectEltpat = 
                oDataSourceEltpats.getData("SELECT ELT.ELTPAT_CODE CODE, ELT.ELTPAT_LIB LIB, ELT.ELTPAT_CODEXT CODEXT " +
                		                   "FROM ELTPAT ELT, ELTPAT_EQUIPEMENT EE " +
                		                   "WHERE "+oCode+" = EE.EQUIPEMENT_CODE " +
                		                   "AND EE.ELTPAT_CODE = ELT.ELTPAT_CODE");
            
            ArrayList<String> oArrPatrimoines = new ArrayList<String>();
            while(resultSelectEltpat.hasNext()) {
                oMapEltpats = resultSelectEltpat.next();
                oArrPatrimoines.add((String) oMapEltpats.get("CODEXT"));
                oArrPatrimoines.add((String) oMapEltpats.get("LIB"));
            }
            oRes.put("patrimoine", oArrPatrimoines);
            oDataSourceEltpats.close();
            
            //maj compteurs
            CGenericJdbcDataSource oDataSourceCompteurs = (CGenericJdbcDataSource) context.getDataSource("dataSourceCompteurs");
            Iterator<Map<String, Object>> resultSelectCompteurs = 
                oDataSourceCompteurs.getData("SELECT C.COMPTEUR_CODE CODE, C.COMPTEUR_LIB LIB " +
                		                     "FROM  COMPTEUR C, COMPTEUR_EQUIPEMENT CE " +
                		                     "WHERE "+oCode+" = CE.EQUIPEMENT_CODE " +
                		                     "AND CE.COMPTEUR_CODE = C.COMPTEUR_CODE");
            
            ArrayList<String> oArrCompteurs = new ArrayList<String>();
            while(resultSelectCompteurs.hasNext()) {
                oMapCompteurs = resultSelectCompteurs.next();
                oArrCompteurs.add((String) oMapCompteurs.get("LIB"));
            }
            oRes.put("compteur", oArrCompteurs);
            oDataSourceCompteurs.close();
            
            //maj gmaotache
            CGenericJdbcDataSource oDataSourceTaches = (CGenericJdbcDataSource) context.getDataSource("dataSourceTaches");
            Iterator<Map<String, Object>> resultSelectTaches = 
                oDataSourceTaches.getData("SELECT G.GMAOTACHE_LIB LIB "+
                        "FROM GMAOTACHE G,GMAOTACHE_MODEQUIPVERSION G_MV, MODEQUIPVERSION MV, EQUIPEMENT E "+
                        "WHERE "+oCode+" = E.EQUIPEMENT_CODE "+
                        "AND E.MODEQUIPVERSION_CODE = MV.MODEQUIPVERSION_CODE " +
                        "AND MV.MODEQUIPVERSION_CODE = G_MV.MODEQUIPVERSION_CODE " +
                        "AND G_MV.GMAOTACHE_CODE = G.GMAOTACHE_CODE");
            
            ArrayList<String> oArrTaches = new ArrayList<String>();
            while(resultSelectTaches.hasNext()) {
                oMapTaches = resultSelectTaches.next();
                oArrTaches.add((String) oMapTaches.get("LIB"));
            }
            oRes.put("tache", oArrTaches);
            oDataSourceTaches.close();
            
            LOG.info("--->oRes apres ajout: "+oRes.toString());
        }else{
            LOG.info("##################L'objet oRes est nul#########################");
        }
       
        LOG.info("===================>CGmaoEquipementProcessor FIN<======================");
        return oRes;
    }
    
    public Context getContext() {
        return context;
    }


}
