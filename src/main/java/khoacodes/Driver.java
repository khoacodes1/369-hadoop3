package khoacodes;

// ./gradlew run --args="<JobName> <input_path> <output_path>"
public class Driver {
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:/");
        
        String jobName = args[0];


        boolean result = false;

        switch (jobName) {
            case "Task1":

                String inputPath1_1 = args[1];
                String inputPath1_2 = args[2];
                String tempOutput1 = args[3];
                String outputPath1 = args[4];
                
                result = CountryCount.run(inputPath1_1, inputPath1_2, tempOutput1, outputPath1);
                break;
            case "Task2":
                String inputPath2_1 = args[1];
                String inputPath2_2 = args[2];
                String tempOutput2 = args[3];
                String outputPath2 = args[4];
                
                result = CountryURLCount.run(inputPath2_1, inputPath2_2, tempOutput2, outputPath2);
                break;
            case "Task3":
                String inputPath3_1 = args[1];
                String inputPath3_2 = args[2];
                String tempOutput3 = args[3];
                String outputPath3 = args[4];
                
                result = URLCountryList.run(inputPath3_1, inputPath3_2, tempOutput3, outputPath3);
                break;

            default:
                System.exit(1);
        }

        if (result)
        {
            System.out.print("Job succeeded");
        }

        System.exit(result ? 0 : 1);

    }
}