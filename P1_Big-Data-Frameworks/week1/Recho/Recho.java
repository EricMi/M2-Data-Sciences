/**
 * Created by mi on 22/09/2017.
 */
public class Recho {
    public static void main(String args[]) {
        for (int i = args.length - 1; i >= 0; i--) {
            if (i != args.length - 1)
                System.out.print(" ");
            System.out.print(args[i]);
        }
        System.out.println();
    }
}
