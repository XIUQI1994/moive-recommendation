/**
 * Driver to run the recommender system on Hadoop
 * To run the code:
 *
 * $: cd RecommenderSystem/
 * $: hdfs dfs -mkdir /input
 * $: hdfs dfs -put input/* /input
 * $: cd src/main/java/
 * $: hadoop com.sun.tools.javac.Main -d class/ *.java
 * $: cd class/
 * $: jar cf recommender.jar *.class
 * $: hadoop jar recommender.jar Driver /input/input /dataDividedByUser /coOccurrenceMatrix /Normalize /averageRating /Multiplication /Sum /recommend 5

 * $: hdfs dfs -cat /Sum/*
 *
 */
//MovieBigDataSet/input/ output/dataDividedByUser/ output/coOccurrenceMatrix/ output/Normalize/ output/averageRating/ output/Multiplication/ output/Sum/ output/recommend/ 5

public class Driver {
    public static void main(String[] args) throws Exception {

        // args[0]: original user rating folder, e.g., /input
        // args[1]: output of the data divided by userID, e.g., /dataDividedByUser
        // args[2]: un-normalized cooccurrence matrix folder, e.g., /coOccurrenceMatrix
        // args[3]: normalized cooccurrence matrix folder, e.g., /Normalize
        // args[4]: averaged user rating list folder, e.g., /averageRating
        // args[5]: matrix multiplication output folder, e.g., /Multiplication
        // args[6]: matrix multiplication sum output folder, e.g., /Sum
        // args[7]: recommend outpur, e.g., /recommend
        // args[8]: number of recommend movies, e.g., 5
        //args /input /dataDivided /cocurre /Normal /average /Multip /Sum

        // begin the program
        DivideDataByUserID divideDataByUserID = new DivideDataByUserID();
        CooccurrenceMatrix cooccurrenceMatrix = new CooccurrenceMatrix();
        Normalization normalization = new Normalization();
        AverageRating averageRating = new AverageRating();
        MatrixMultiplication matrixMultiplication = new MatrixMultiplication();
        MatrixSum matrixSum = new MatrixSum();
        Recommend recommend = new Recommend();

        // get the input and out path for each class
        String rawInputPath = args[0];  // e.g., /input
        String userMoveListPath = args[1];  // e.g., /dataDividedByUser
        String cooccurrenceMatrixPath = args[2];  // e.g., /coOccurrenceMatrix
        String normalizedMatrixPath = args[3];  // e.g., /Normalize
        String averageRatingPath = args[4];  // e.g., /averageRating
        String rawMatrixMultiplicationPath = args[5];  // e.g., /Multiplication
        String matrixMultiplicationSumPath = args[6];  // e.g., /Sum
        String recommendPath = args[7];
        String numberOfRecommendedMovies = args[8];

        // build the input path
        String[] path1 = {rawInputPath, userMoveListPath};
        String[] path2 = {userMoveListPath, cooccurrenceMatrixPath};
        String[] path3 = {cooccurrenceMatrixPath, normalizedMatrixPath};
        String[] path4 = {rawInputPath, averageRatingPath};
        String[] path5 = {averageRatingPath, normalizedMatrixPath, rawInputPath, rawMatrixMultiplicationPath};
        String[] path6 = {rawMatrixMultiplicationPath, matrixMultiplicationSumPath};
        String[] path7 = {matrixMultiplicationSumPath, rawInputPath, recommendPath,numberOfRecommendedMovies};

        // run recommender system
        divideDataByUserID.main(path1);  // MapReduce Job 1
        cooccurrenceMatrix.main(path2);  // MapReduce Job 2
        normalization.main(path3);  // MapReduce Job 3
        averageRating.main(path4);  // MapReduce Job 4
        matrixMultiplication.main(path5);  // MapReduce Job 5
        matrixSum.main(path6);  // MapReduce Job 6
        recommend.main(path7);  // MapReduce Job 7
    }

}
