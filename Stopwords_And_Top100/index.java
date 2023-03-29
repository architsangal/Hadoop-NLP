import java.io.File;
import java.util.Scanner;

class index
{
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.print("[");
        while(sc.hasNextLine())
        {
            String s = sc.nextLine();
            if(s.length() != 0)
            System.out.print("\"" + s  + "\", ");
        }
        System.out.println("]");
        sc.close();
    }
}