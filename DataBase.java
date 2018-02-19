import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class DataBase{
    private String csvFile;
    private String[] headers;
    private String[] col;
    private String[] dataType;

    DataBase(){}

    DataBase(String csvFile){
        this.csvFile = csvFile;
    }

    public void getHeaderType(){
        System.out.println("\n**************getting type of header");
        BufferedReader br = null;
        String row = "";
        boolean numeric;
        int pos= 0;

        int skip = 0;
        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((row = br.readLine()) != null) {
                if(skip == 0){
                    skip++;
                    continue;
                }

                if(skip == 2)
                    break;

                // use comma as separator
                this.col = row.split(",");
                this.dataType = new String[this.headers.length];
                for(String sub: this.col){
                    numeric = sub.matches("-?\\d+(\\.\\d+)?");
                    if(sub.matches("-?\\d+(\\.\\d+)?"))
                        dataType[pos] = "Number";
                    else if(sub.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}"))
                        dataType[pos] = "Date";
                    else
                        dataType[pos] = "String";
                    System.out.println(this.headers[pos]+" - "+this.dataType[pos]);
                    pos++;
                }
                skip++;
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public void readFileHeader(){
        System.out.println("\n**************reading header of file");
        BufferedReader br = null;
        String row = "";

        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((row = br.readLine()) != null) {
               this.headers = row.split(",");
               break;
            }

            for(String sub: headers){
                System.out.println(sub);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public void executeQuery(QueryParameter query){
        String[] fields;
        int loc = 0;
        int pos = 0;
        System.out.println("\nYour query: "+query.getQuery());
        fields = query.getFields();
        int[] targetField = new int[fields.length];
        for(String field:fields){
            pos = 0;
            for(String header:this.headers){
                if(field.equals(header))
                    targetField[loc++] = pos;
                pos++;
            }
        }

        System.out.print("\nLocations to read: ");
        for(int target:targetField)
            System.out.print(target+" ");



    }
}