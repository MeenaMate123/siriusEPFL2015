package info.ephyra.search.searchers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import info.ephyra.io.MsgPrinter;
import info.ephyra.questionanalysis.QuestionInterpretation;
import info.ephyra.search.Result;
import info.ephyra.util.FileUtils;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;

public class SparqlKM extends KnowledgeMiner{
	
	/** Maximum total number of search results. */
	private static final int MAX_RESULTS_TOTAL = 100;
	/** Maximum number of search results per query. */
	private static final int MAX_RESULTS_PERQUERY = 10;
	/** Map of templates: Property -> Set of templates **/
	private static Map<String, HashSet<String[]>> templates = new HashMap<String, HashSet<String[]>>();
	/** SPARQL endpoint address. */
	//String sparqlEndpoint = "http://localhost:8890/sparql";
	String sparqlEndpoint = "http://192.168.9.122:8890/sparql";

	@Override
	protected int getMaxResultsTotal() {
		return MAX_RESULTS_TOTAL;
	}

	@Override
	protected int getMaxResultsPerQuery() {
		return MAX_RESULTS_PERQUERY;
	}

	@Override
	public KnowledgeMiner getCopy() {
		return new SparqlKM();
	}

	@Override
	protected Result[] doSearch() {
		HashSet<Result> tmpRes = new HashSet<Result>();
		
		//int i = 0;
		
		for(QuestionInterpretation qi : query.getAnalyzedQuestion().getInterpretations()){
			// Get the query templates
			String request;
			String resultVar;
			//MsgPrinter.printStatusMsg("Round in one thread?: " + i);
			//++i;
			//MsgPrinter.printStatusMsg("Property in the interpretation: " + qi.getProperty());
			//MsgPrinter.printStatusMsg("Tempates contain the key: " + templates.containsKey(qi.getProperty()));
			//Safety against empty and not defined templates.
			HashSet<String[]> sparqlRequestS = null;
			if (templates.containsKey(qi.getProperty())){
				sparqlRequestS = templates.get(qi.getProperty());
				
				for (String[] template : sparqlRequestS){
					if (template[0] != null){
						request = template[0];
						request = request.replace("<TARGET>", "\"" + qi.getTarget() + "\"");
						MsgPrinter.printStatusMsg("Request:" + request);
						
	
						if (template[1] != null){
							resultVar = template[1];
							//MsgPrinter.printStatusMsg(resultVar);
							
							Query querySPARQL = QueryFactory.create(request, Syntax.syntaxARQ) ;
							
							QueryEngineHTTP httpQuery = new QueryEngineHTTP(sparqlEndpoint,querySPARQL);
						    // execute a Select query
						    ResultSet results = httpQuery.execSelect();
						    while (results.hasNext()) {
						      QuerySolution solution = results.next();
						      // get the value of the variables in the select clause
						      String expressionValue = solution.get(resultVar).asLiteral().getLexicalForm();
						      // print the output to stdout
						      MsgPrinter.printStatusMsg(expressionValue);
						      tmpRes.add(new Result(expressionValue, query));
						    }
							//tmpRes.add(new Result("Suppose found answer", query));
							
						} else {
							MsgPrinter.printStatusMsg("Result variable not found:(");
							//return new Result[0];
						}
					
					} else {
						MsgPrinter.printStatusMsg("Request no found:(");
						//return new Result[0];
					}
				}
			
			} else {
				MsgPrinter.printStatusMsg("Template not found :(");
				//return new Result[0];
			}
			
		}
		//System.out.println(query.getAnalyzedQuestion().getInterpretations()[0]);	
		return tmpRes.toArray(new Result[tmpRes.size()]);
	}

	public static boolean loadPatterns(String dir) {
		File[] files = FileUtils.getFiles(dir);
		
		try {
			BufferedReader in;
			String prop, templ, var;
			String[] templatesForProp;
			HashSet<String[]> templatesSet;
			
			for (File file : files) {
				MsgPrinter.printStatusMsg("  ...for " + file.getName());
				
				prop = file.getName();
				in = new BufferedReader(new FileReader(file));
				
				// total number of passages used to assess the patterns
				//passages = Integer.parseInt(in.readLine().split(" ")[1]);
				//nOfPassages.put(prop, passages);
				
				templatesSet = new HashSet<String[]>();
				
				//templatesForProp = new String[2];
				while (in.ready()) {
					//in.readLine();
					// template
					
					templatesForProp = new String[2];
					
					templ = in.readLine();
					
					templatesForProp[0] = templ;
					
					var = in.readLine();
					
					templatesForProp[1] = var;
					
					templatesSet.add(templatesForProp);
					
				}
				templates.put(prop, templatesSet);
				in.close();
			}
			
			MsgPrinter.printStatusMsg("Loaded:");
			for(String propPrint : templates.keySet()){
				MsgPrinter.printStatusMsg("Property: " + propPrint);
				for(String[] templToPrint :templates.get(propPrint)){
					for(String tempPart : templToPrint){
						MsgPrinter.printStatusMsg(tempPart);
					}
				}
			}
			MsgPrinter.printStatusMsg("  ...done");
		} catch (IOException e) {
			return false;
		}
		
		return true;
	}

}
