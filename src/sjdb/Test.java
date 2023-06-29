package sjdb;

import java.io.*;
import java.util.ArrayList;
import sjdb.DatabaseException;

public class Test {
	private Catalogue catalogue;

	public Test() {
	}

	public static void main(String[] args) throws Exception {
		Catalogue catalogue = createCatalogue();
		Inspector inspector = new Inspector();
		Estimator estimator = new Estimator();

		Operator plan = query(catalogue);
		plan.accept(estimator);
		plan.accept(inspector);
		System.out.println("------------------------------");
		Optimiser optimiser = new Optimiser(catalogue);
		Operator planopt = optimiser.optimise(plan);
		planopt.accept(estimator);
		planopt.accept(inspector);
	}

	public static Catalogue createCatalogue() {
		Catalogue cat = new Catalogue();

		cat.createRelation("A", 1000);
		cat.createAttribute("A", "a1", 100);
		cat.createAttribute("A", "a2", 15);
		cat.createAttribute("A", "a3", 15);
//relation B
		cat.createRelation("B", 15);
		cat.createAttribute("B", "b1", 150);
		cat.createAttribute("B", "b2", 100);
		cat.createAttribute("B", "b3", 5);

//relation C for joins
		cat.createRelation("C", 20);
		cat.createAttribute("C", "c1", 20);
		cat.createAttribute("C", "c2", 150);
		cat.createAttribute("C", "c3", 300);
//relation D for joins
		cat.createRelation("D", 20);
		cat.createAttribute("D", "d1", 20);
		cat.createAttribute("D", "d2", 150);
		cat.createAttribute("D", "d3", 3000);

		cat.createRelation("E", 20);
		cat.createAttribute("E", "e1", 20);
		cat.createAttribute("E", "e2", 150);
		cat.createAttribute("E", "e3", 3000);
		return cat;
	}

	public static Operator query(Catalogue cat) throws Exception {
        Scan a = new Scan(cat.getRelation("A"));
        Scan b = new Scan(cat.getRelation("B"));
        Scan c = new Scan(cat.getRelation("C"));
        Scan d = new Scan(cat.getRelation("D"));
        Scan e = new Scan(cat.getRelation("E"));
        Product product1 = new Product(a, b);
        Product product2 = new Product(product1, c);
        Product product3 = new Product(product2, d);
        Product product4 = new Product(product3, e);
        Select select1 = new Select(product4, new Predicate(new Attribute("a1"), "1"));
        Select select2 = new Select(select1, new Predicate(new Attribute("c1"), "2")); //1
        Select select3 = new Select(select2, new Predicate(new Attribute("a2"), new Attribute("b2"))); //4
        Select select4 = new Select(select3, new Predicate(new Attribute("b1"), new Attribute("c2")));
        Select select5 = new Select(select4, new Predicate(new Attribute("a2"), new Attribute("c2"))); //3
        Select select6 = new Select(select5, new Predicate(new Attribute("c3"), new Attribute("c2"))); //2


		ArrayList<Attribute> atts = new ArrayList<Attribute>();
		atts.add(new Attribute("a1"));
		atts.add(new Attribute("b1"));
		Project project = new Project(select6, atts);
		return project;
	}


}

