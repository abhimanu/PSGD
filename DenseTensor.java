import java.lang.*;
import java.io.*;
import java.util.*;
import java.util.zip.*;
import java.text.*;

public class DenseTensor implements Tensor {
	int N;
	int M;
	int P;
	float[][][] data;
	int iter;

	public DenseTensor(int n, int m) {
		this(n,m,1);
	}

	public DenseTensor(int n, int m, int p) {
		N = n;
		M = m;
		P = p;
		data = new float[N][M][P];
		iter = -1;
		reset();
	}
	
	public DenseTensor(int n, int m, int p, float val) {
		N = n;
		M = m;
		P = p;
		data = new float[N][M][P];
		iter = -1;
		reset(val);
	}

	public float get(int i, int j) {
		return get(i,j,0);
	}

	public float get(int i, int j, int k) {
		if(i >= 0 && j >= 0 && k >= 0 && i < N && j < M && k < P)
			return data[i][j][k];
		System.out.println("ERROR (DenseTensor): out of bounds on:" + i + ", " + j + ", " + k);
		return -1; // Throw error?
	}

	public void set(int i, int j, float value) {
		set(i,j,0,value);
	}

	public void set(int i, int j, int k,  float value) {
		if(i >= 0 && j >= 0 && k >= 0 && i < N && j < M && k < P)
			data[i][j][k] = value;
		else
			System.out.println("ERROR (DenseTensor): out of bounds on write " + i + "/" + N + ", " + j + "/" + M + ", " + k + "/" + P + ": " + value);
		// else throw error?
	}

	public void reset() {
		Random r = new Random();

		for(int i = 0; i < N; i++) {
			for(int j = 0; j < M; j++) {
				for(int k = 0; k < P; k++) {
					data[i][j][k] = r.nextFloat() * 2 - 1.0f;
					if(Float.isNaN(data[i][j][k])) {
						System.out.println("NaN Error on Reset");
					}
				}
			}
		}

	}

	public void reset(float val) {

		for(int i = 0; i < N; i++) {
			for(int j = 0; j < M; j++) {
				for(int k = 0; k < P; k++) {
					data[i][j][k] = val;
				}
			}
		}
	}


}
