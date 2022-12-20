package edu.mcw.rgd.rarediseases.process;

import edu.mcw.rgd.dao.impl.AnnotationDAO;
import edu.mcw.rgd.dao.impl.AssociationDAO;
import edu.mcw.rgd.dao.impl.OntologyXDAO;
import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.datamodel.Strain;
import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.datamodel.ontologyx.TermSynonym;
import edu.mcw.rgd.rarediseases.ESClient;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.*;

import java.util.*;
import java.util.stream.Collectors;

public class ProcessFile {

    public void insertFromFile(String file) throws Exception {
        AnnotationDAO annotationDAO=new AnnotationDAO();
        FileInputStream fs=new FileInputStream(new File(file));
        XSSFWorkbook workbook=new XSSFWorkbook(fs);
        XSSFSheet sheet=workbook.getSheet("diseases");
        Iterator<Row> rowIterator=sheet.iterator();
        OntologyXDAO xdao=new OntologyXDAO();
        BufferedWriter writer = new BufferedWriter(new FileWriter("data/output.txt"));

        List<Term> terms=new ArrayList<>();
               terms.addAll( xdao.getAllActiveTermDescendants("DOID:4"));
               terms.addAll(xdao.getAllActiveTermDescendants("HP:0000001"));
               terms.addAll(xdao.getAllActiveTermDescendants("MP:0000001"));
        writer.write("Disease"+"\t"+"Acc_Id" +"\t"
                + "Annotations"+"\t" + "Annotated Genes"+"\t"+"Strains\t" +"Matched By"+"\n");
        List<String> unmatchedTerms=new ArrayList<>();



        while(rowIterator.hasNext()) {
            Row row = rowIterator.next();
            boolean flag=false;
            Iterator<Cell> cellIterator = row.cellIterator();
            if (row.getRowNum() != 0) {

                while (cellIterator.hasNext()) {

                    Cell cell = cellIterator.next();
                    int colIndex = cell.getColumnIndex();
                    if (colIndex == 0) {
                        String term=cell.getStringCellValue().trim();

                        for(Term t:terms){
                            int speciesTypeKey=3;
                            if(t.getTerm().toLowerCase().equalsIgnoreCase(term.trim())){
                                if(t.getOntologyId().equalsIgnoreCase("HP")){
                                    speciesTypeKey=2;
                                }
                               List<Gene> genes= annotationDAO.getAnnotatedGenes(Arrays.asList( t.getAccId()), speciesTypeKey);
                               List<Strain> strains= annotationDAO.getAnnotatedStrains(Arrays.asList( t.getAccId()), speciesTypeKey);
                               // SearchResponse sr= getPhenominerData(t.getTerm(), strains.stream().map(Strain::getStrain).collect(Collectors.toList()));
                                writer.write(term+"\t"+t.getAccId() +"\t"+genes.size()+"\t"+genes.stream().map(g->g.getSymbol()+"("+g.getRgdId()+" - "+ SpeciesType.getCommonName(g.getSpeciesTypeKey())+ ")").collect(Collectors.joining(";"))+"\t"
                                        + strains.stream().map(s->s.getStrain()+"("+s.getRgdId()+")").collect(Collectors.joining("|"))
                                        +"\t"+ "term"+"\n"
                               // +"\t"+ sr.getHits().getTotalHits().value
                                );
                                flag=true;
                               // break;
                            }

                        }
                        if(!flag) {
                            unmatchedTerms.add(term);
                        }

                    }
                }

            }
        }
        System.out.println("UNMATCHED COUNT BEFORE SYNONYMS MATCH:"+ unmatchedTerms.size());

        List<String> unmatched=new ArrayList<>();
        List<TermSynonym> synonyms=new ArrayList<>();

        synonyms.addAll(xdao.getActiveSynonyms("RDO"));
        synonyms.addAll(xdao.getActiveSynonyms("MP"));
        synonyms.addAll(xdao.getActiveSynonyms("HP"));
        System.out.println("SYNONYMS SIZE:"+synonyms.size());
        for (String str : unmatchedTerms) {
          boolean flag2=false;
                        for (TermSynonym synonym : synonyms) {
                            if (str.toLowerCase().equalsIgnoreCase(synonym.getName()) ) {
                                int speciesTypeKey=3;
                                if(synonym.getTermAcc().contains("HP")){
                                    speciesTypeKey=2;
                                }
                                List<Gene> genes = annotationDAO.getAnnotatedGenes(Arrays.asList(synonym.getTermAcc()), speciesTypeKey);

                                List<Strain> strains = annotationDAO.getAnnotatedStrains(Arrays.asList(synonym.getTermAcc()), speciesTypeKey);
                                writer.write(str + "\t" + synonym.getTermAcc() + "\t" + genes.size() + "\t"
                                        + strains.stream().map(s -> s.getStrain() + "(" + s.getRgdId() + ")").collect(Collectors.joining("|"))
                                        + "\t" + "synonym" + "\n"
                                );
                                    flag2=true;
                                    break;
                            }
                        }



            if(!flag2){
                unmatched.add(str);
            }

        }
       System.out.println("UNMATCHED COUNT AFTER SYNONYMS MATCH:"+ unmatched.size());
        writer.close();
        fs.close();



    }
    public SearchResponse getPhenominerData(String term, List<String> rsTerms) throws Exception {
        SearchResponse sr=getSearchResponse(term, rsTerms);
       return sr;
    }
    public SearchResponse getSearchResponse(String cmoTerm, List<String> rsTerms) throws Exception{

        BoolQueryBuilder builder=this.boolQueryBuilder(cmoTerm, rsTerms);
        SearchSourceBuilder srb=new SearchSourceBuilder();
        srb.query(builder);
        srb.size(10000);
        SearchRequest searchRequest=new SearchRequest("phenominer_index_dev");
        searchRequest.source(srb);

        return ESClient.getClient().search(searchRequest, RequestOptions.DEFAULT);

    }
    public BoolQueryBuilder boolQueryBuilder(String cmoTerms, List<String> rsTerms){
        BoolQueryBuilder builder=new BoolQueryBuilder();
        builder.must(this.getDisMaxQuery( cmoTerms));
          //  builder.filter(QueryBuilders.termsQuery("rsTermAcc.keyword",rsTerms.toArray()));
            return builder;
    }
    public QueryBuilder getDisMaxQuery(String cmoTerm ){
        DisMaxQueryBuilder dqb=new DisMaxQueryBuilder();
        dqb.add(QueryBuilders.termsQuery("cmoTermAcc.keyword", cmoTerm));

        return dqb;

    }
}
