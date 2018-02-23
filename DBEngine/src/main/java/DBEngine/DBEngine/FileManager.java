package DBEngine.DBEngine;

public class FileManager {
  private BufferedReader bufferedReader;
  private FileReader fileReader;
  private FileWriter fileWriter;
  private ArrayList<String> colList;

  FileManager(){
    this.bufferedReader = null;
    this.fileReader = null;
    this.fileWriter = null;
    this.colList = null;
  }

  public void load(String fileName) throws FileNotFoundException{
    try{
      // check if file exists
      fileReader = new FileReader(fileName);
    } catch(FileNotFoundException e){
      // throw exception if file does not exist
      throw FileNotFoundException;
    } finally{
      // close resource
      fileReader.close();
    }
  }

  public ArrayList<String> getFirstRow(){
    String[] columns = null;
    String row = "";
    int rowCount = 0;
    try{
      // load file
      fileReader = new FileReader(fileName);
      // initialize buffer reader to read lines
      bufferedReader = new BufferedReader(fileReader);
        while((row = bufferedReader.readLine()) != null){
          // read first row then exit
          if(rowCount == 1)
            break;
          // split first line based on comma
          columns = row.split(",");
          for(String column:columns)
            // add every column to list
            colList.add(column);
          // mark completition of reading row
          rowCount++;
        }
      } catch(FileNotFoundException e){
          throw FileNotFoundException;
      } finally{
          // close resource
          fileReader.close();
      }
  }

}
