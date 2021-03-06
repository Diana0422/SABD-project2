package com.diagiac.kafka.utils;

import com.diagiac.kafka.bean.SensorDataModel;
import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Reads the csv and save it on a list of SensorDataModel
 */
public class ReadCsv {

    private final String csvFileName;
    private List<SensorDataModel> recordList;

    public ReadCsv(String csvName) {
        this.csvFileName = csvName;
    }

    public List<SensorDataModel> readCSVFile() {
        try {
            // replace semicolon with the comma separator
            replaceSemicolonSeparator();
            // read the csv into the bean
            CSVReader csvReader = new CSVReader(new FileReader(csvFileName));
            CsvToBean<SensorDataModel> csvToBean = new CsvToBeanBuilder<SensorDataModel>(csvReader)
                    .withType(SensorDataModel.class)
                    .withIgnoreEmptyLine(true).build();

            recordList = csvToBean.parse();
            csvReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return recordList;
    }

    private void replaceSemicolonSeparator() throws IOException {
        // Change separator
        StringBuilder oldContent = new StringBuilder();
        BufferedReader reader = new BufferedReader(new FileReader(csvFileName));
        String line = reader.readLine();
        while (line != null) {
            oldContent.append(line).append("\n");
            line = reader.readLine();
        }
        reader.close();

        String newContent = oldContent.toString().replaceAll(";", ",");
        FileWriter fileWriter = new FileWriter(csvFileName);
        fileWriter.write(newContent);

        fileWriter.close();
    }
}
