import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AmazonRatingReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<double[]> data = new ArrayList<>();

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length == 4) {
                double y = Double.parseDouble(parts[0]);
                double x1 = Double.parseDouble(parts[1]);
                double x2 = Double.parseDouble(parts[2]);
                double x3 = Double.parseDouble(parts[3]);
                data.add(new double[]{1, x1, x2, x3, y}); // 1 for bias term
            }
        }

        int N = data.size();
        if (N == 0) return;

        double[][] X = new double[N][4];
        double[] y = new double[N];

        for (int i = 0; i < N; i++) {
            X[i][0] = data.get(i)[0]; // bias term
            X[i][1] = data.get(i)[1];
            X[i][2] = data.get(i)[2];
            X[i][3] = data.get(i)[3];
            y[i] = data.get(i)[4];
        }

        double[][] XT_X = new double[4][4];
        double[] XT_y = new double[4];

        for (int i = 0; i < N; i++) {
            for (int j = 0; j < 4; j++) {
                XT_y[j] += X[i][j] * y[i];
                for (int k = 0; k < 4; k++) {
                    XT_X[j][k] += X[i][j] * X[i][k];
                }
            }
        }

        double[] weights = solveLinearSystem(XT_X, XT_y);

        double sumError = 0.0;
        for (int i = 0; i < N; i++) {
            double predicted = 0.0;
            for (int j = 0; j < 4; j++) {
                predicted += X[i][j] * weights[j];
            }
            sumError += Math.abs(predicted - y[i]);
        }

        double mae = sumError / N;
        double accuracy = 100 - ((mae / 5.0) * 100);

        context.write(new Text("Model Weights (w0, w1, w2, w3):"), new Text(arrayToString(weights)));
        context.write(new Text("Mean Absolute Error (MAE):"), new Text(String.format("%.4f", mae)));
        context.write(new Text("Estimated Accuracy:"), new Text(String.format("%.2f%%", accuracy)));
    }

    private String arrayToString(double[] arr) {
        StringBuilder sb = new StringBuilder();
        for (double d : arr) sb.append(String.format("%.4f ", d));
        return sb.toString();
    }

    private double[] solveLinearSystem(double[][] A, double[] b) {
        int n = b.length;
        for (int i = 0; i < n; i++) {
            int max = i;
            for (int j = i + 1; j < n; j++) {
                if (Math.abs(A[j][i]) > Math.abs(A[max][i])) max = j;
            }
            double[] tempA = A[i]; A[i] = A[max]; A[max] = tempA;
            double tempB = b[i]; b[i] = b[max]; b[max] = tempB;

            for (int j = i + 1; j < n; j++) {
                double factor = A[j][i] / A[i][i];
                b[j] -= factor * b[i];
                for (int k = i; k < n; k++) {
                    A[j][k] -= factor * A[i][k];
                }
            }
        }

        double[] x = new double[n];
        for (int i = n - 1; i >= 0; i--) {
            x[i] = b[i];
            for (int j = i + 1; j < n; j++) {
                x[i] -= A[i][j] * x[j];
            }
            x[i] /= A[i][i];
        }
        return x;
    }
}
