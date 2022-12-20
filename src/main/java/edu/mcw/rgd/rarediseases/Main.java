package edu.mcw.rgd.rarediseases;

import edu.mcw.rgd.dao.impl.OntologyXDAO;
import edu.mcw.rgd.rarediseases.process.ProcessFile;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("VERSION-"+ System.currentTimeMillis());
        ProcessFile process=new ProcessFile();

        try {

            process.insertFromFile("data/rare_disease_list_orphanet.xlsx");
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(ESClient.getClient()!=null)
            ESClient.destroy();
        System.out.println("Done!!");
    }

}
