package GP;
import java.util.*;

public class myGraph {
	// stracture of the graph

	protected HashMap<Integer, HashSet<Integer>> graph = new HashMap<Integer, HashSet<Integer>>();

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
		if (!graph.containsKey(vertex1)) {

			graph.put(vertex1, new HashSet<Integer>());
		}

		if (!graph.containsKey(vertex2)) {
			graph.put(vertex2, new HashSet<Integer>());
		}
		graph.get(vertex1).add(vertex2);
		graph.get(vertex2).add(vertex1);
		addVertex(vertex1);
		addVertex(vertex2);

	}

	// returns the neighbors of the vertex
	public ArrayList<Integer> getNeighbors(Integer v) {
		return new ArrayList<Integer>(graph.get(v));
	}

	// returns the grade of v
	public int degree(Integer v) {
		return getNeighbors(v).size();
	}

	// simple algoritm triangle counting based on edge sorting
	public double countNaiveTriangles(int p)
	{
		double counter = 0;

		for (int s : vertex) {
			ArrayList<Integer> neighbors = getNeighbors(s);

			for (int ii = 0; ii < neighbors.size(); ii++) {
				for (int iii = 0; iii < neighbors.size(); iii++) {
					int j = neighbors.get(ii);
					int k = neighbors.get(iii);
					if ((s < j) && (j < k) && graph.get(k).contains(j)) {

						double z = 1.0;
						if ((s % p) == (j % p) && (k % p) == (j % p)) {
							z = combination(s % p, 2) + (s % p)
									* (p - s % p - 1)
									+ combination(p - s % p - 1, 2);
						} else {

							if (s % p == j % p || k % p == j % p
									|| s % p == k % p) {
								z = p - 2;
							}
						}
						
							counter = counter + 1.0 / z;
						
					}

				}
			}
		}

		return counter;
	}

	// first neighbor with min grade 
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

	// first neighbor with min grade of v
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

	
	public long combination(int n, int k) {
		
		return permutation(n) / (permutation(k) * permutation(n - k));
	}

	
	public long permutation(int i) {
		long k = 1;
		for (int j = 2; j <= i; j++) {
			k = k * j;
		}
		return k;
	}

	// compact forward
	public double countTrianglesCompFor(int p) {
		// sort the vertexes by their grades
		java.util.Collections.sort(vertex, new java.util.Comparator<Integer>() {

			@Override
			public int compare(Integer o1, Integer o2) {
				return (degree(o1) > degree(o2) ? -1
						: (degree(o1) == degree(o2) ? 0 : 1));
			}
		});

		// compact-forward algorithm
		double counter = 0.0;
		double z;
		int templ, tempk, tempi;

		int l;
		ArrayList<Integer> neighbors;
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
								z = 1.0;
								// compute z (instractions of paper)
								if ((templ % p) == (tempk % p)
										&& (tempk % p) == (tempi % p)) {
									z = combination(tempi % p, 2) + (tempi % p) * (p - (tempi%p) - 1) + combination((p - (tempi % p) - 1), 2);
									
								} else {
									if (tempi % p == templ % p
											|| templ % p == tempk % p
											|| tempi % p == tempk % p) {
										z = p - 2;
										
									}
								}
								
								counter = counter + 1.0 / z;
								
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