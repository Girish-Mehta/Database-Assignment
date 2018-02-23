package DBEngine.DBEngine;

public class DataBaseManager {

	private ArrayList<String> fields = null;
	private ArrayList<String> row = null;
	private ArrayList<String> dataType = null;

	public boolean executeQuery(QueryParameter queryParameter){

		try{
			//LoadFields()
			LoadFields(queryParameter.getFileName);
			//LoadDataTypes()
			LoadDataTypes();
		} catch(FileNotFoundException e){
			throw FileNotFoundException;
		}


		return false;
	}

	private void LoadFields(String fileName) {
		// create object
		DataBaseManager basemanager = new DataBaseManager();
		try{
			// load file
			baseManager.load(fileName);
		} catch(FileNotFoundException e){
			throw FileNotFoundException;
		}
		// read first row and save column names in list
		this.fields = baseManager.getFirstRow();
	}

	private void LoadDataTypes() {
		String column = "";
		String string = "";
		Integer num = 0;
		Date date;

		Iterator iterator = fields.Iterator();
		while(iterator.hasNext()){
			column = iterator.next();
			try{
				date = new date(column);
				dataType.add("Date");
			} catch(Exception e){
				try{
					num = (Integer)column;
					dataType.add("Number");
				} catch(Exception ee){
					string = column;
					dataType.add("String");
				}
			}
		}
	}

}
