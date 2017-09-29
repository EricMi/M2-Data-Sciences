/**
 * Created by mi on 22/09/2017.
 */

import java.util.Random;
import java.io.*;

public class GuessNumber {
    static int MIN = 0;
    static int MAX = 10000;

    public static void main(String args[]) throws IOException {
        Random rand = new Random();
        int num = rand.nextInt(MAX - MIN + 1) + MIN;
        System.out.printf("I have chosen a number from %d to %d, can you guess it?\n", MIN, MAX);

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        int ans = -1;
        int i = 0;
        while (true) {
            try {
                ans = Integer.parseInt(in.readLine());
                i++;
                if (ans < num) {
                    System.out.println("It's smaller.");
                } else if (ans > num) {
                    System.out.println("It's bigger.");
                } else {
                    System.out.printf("Bravo! You get it in %d guesses!\n", i);
                    return;
                }
            } catch (NumberFormatException e) {
                System.err.println("Input error: please enter an integer.");
            }
        }
    }
}
