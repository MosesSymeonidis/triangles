package ttp;
import java.util.*;

public class myGraph { 
	// δομή που κρατάει το γράφο
	
	// ένα hashMap της μορφής<καρυφή,<set γειτώνων>>
	protected HashMap<Integer, HashSet<Integer>> graph = new HashMap<Integer, HashSet<Integer>>();
	
	// βοηθητική δομή για ταξινόμηση κορυφών με βάση το βαθμό τους
	protected ArrayList<Integer> vertex = new ArrayList<Integer>();

	public myGraph() {
	}

	// προσθήκη μιας κορυφής
	public void addVertex(int newVertex) {
		if (!vertex.contains(newVertex)) {// αν δεν υπάρχει η κορυφή
			vertex.add(newVertex);
		}
	}

	// προσθήκη μιας ακμής στο γράφο
	public void addEdge(int vertex1, int vertex2) {
		if (!graph.containsKey(vertex1)) {// αν δέν υπάρχει η κορυφή στο hashmap
											// γειτνίασης
			
			graph.put(vertex1, new HashSet<Integer>());// προσθεσέ την ακμή
		}

		if (!graph.containsKey(vertex2)) {
			graph.put(vertex2, new HashSet<Integer>());
		}
		graph.get(vertex1).add(vertex2);// άλλαξε το set των γειτώνων
		graph.get(vertex2).add(vertex1);
		addVertex(vertex1);
		addVertex(vertex2);

	}

	// επιστρέφει ένα ArrayList με τους γέιτωνες της v
	public ArrayList<Integer> getNeighbors(Integer v) {
		return new ArrayList<Integer>(graph.get(v));
	}

	// επιστρέφει το βαθμό της v
	public int degree(Integer v) {
		return getNeighbors(v).size();
	}

	public double countNaiveTriangles(int p)// απλός αλγόριθμος καταγραφής
											// τριγώνων με ταξινόμηση κορυφών με
											// βάση το όνομα
	{
		double counter = 0.0;

		for (int s : vertex) {
			ArrayList<Integer> neighbors = getNeighbors(s);

			for (int ii = 0; ii < neighbors.size(); ii++) {
				for (int iii = 0; iii < neighbors.size(); iii++) {
					int j = neighbors.get(ii);
					int k = neighbors.get(iii);
					if ((s < j) && (j < k) && graph.get(k).contains(j)) {

						if ((s % p) == (j % p) && (k % p) == (j % p)) {
							// καταγραφή τριγώνων που βρίσκονται σε 1-partition
							counter = counter + (1.0 / (p - 1));
						} else {
							counter = counter + 1;// καταγραφή όλων των
													// υπόλοιπων
						}
					}
				}
			}
		}

		return counter;
	}

	// -----------------------------------------------------------------------------------------------
	// πρώτος γείτωνας (γείτωνας με ελάχιστο βαθμό) του v μέτα τον j
	public int n_n_i(int v, int j) {
		int index, min = vertex.size();
		ArrayList<Integer> neighbors = getNeighbors(v);
		for (int i = 0; i < neighbors.size(); i++) {
			index = vertex.indexOf(neighbors.get(i));
			if (min > index && index > j) {
				min = index;
			}
		}
		return min;
	}

	// πρώτος γείτωνας (γείτωνας με ελάχιστο βαθμό) του v
	public int f_n_i(int v) {
		int index, min = vertex.size();
		ArrayList<Integer> neighbors = getNeighbors(v);
		for (int i = 0; i < neighbors.size(); i++) {
			index = vertex.indexOf(neighbors.get(i));
			if (min > index) {
				min = index;
			}
		}

		return min;
	}

	// compact forward
	public double countTrianglesCompFor(int p) {
		// ταξινόμηση κορυφών με βάση τον βαθμό τους
		java.util.Collections.sort(vertex, new java.util.Comparator<Integer>() {

					@Override
					public int compare(Integer o1, Integer o2) {
						return (degree(o1) > degree(o2) ? -1
								: (degree(o1) == degree(o2) ? 0 : 1));
					}
				});

		// αλγόριθμος compact-forward
		double counter = 0.0;
		
		int l;
		ArrayList<Integer> neighbors;
		int templ;
		int tempk;
		int tempi;
		for (int i = 0; i < vertex.size(); i++) {
			neighbors = getNeighbors(vertex.get(i));
			for (int s = 0; s < neighbors.size(); s++) {
				l = vertex.indexOf(neighbors.get(s));
				if (l < i) {
					int j = f_n_i(vertex.get(i));
					int k = f_n_i(vertex.get(l));
					while ((j < l) && (k < l)) {
						if (j < k) {
							j = n_n_i(vertex.get(i), j);
						} else {
							if (k < j) {
								k = n_n_i(vertex.get(l), k);
							} else {
								templ = vertex.get(l);
								tempk = vertex.get(k);
								tempi = vertex.get(i);

								if (templ % p == tempk % p
										&& tempk % p == tempi % p) {// καταγραφή τριγώνων που βρίσκονται σε 1-partition
									counter = counter + (1.0 / (p - 1));

								} else {
									counter = counter + 1.0;
								}// καταγραφή όλων των υπόλοιπων

								j = n_n_i(tempi, j);
								k = n_n_i(templ, k);

							}
						}
					}

				}
			}

		}
		return counter;
	}
}