import java.util.*;

class Top
{
    public static void main(String[] args)
    {
        Scanner sc = new Scanner(System.in);
        HashMap<Integer,ArrayList<String>> map = new HashMap<Integer,ArrayList<String>>();
        PriorityQueue<Integer> pq = new PriorityQueue<>(Collections.reverseOrder());
        
        while(sc.hasNext())
        {
            String val = sc.next();
            int DF = sc.nextInt();
            pq.add(DF);
            if(map.containsKey(DF))
                map.get(DF).add(val);
            else
            {
                map.put(DF, new ArrayList<String>());
                map.get(DF).add(val);
            }
        }

        // System.out.println(map.get(pq.remove()));

        int count = 0;
        ArrayList<String> answer = new ArrayList<String>();
        ArrayList<Integer> df = new ArrayList<Integer>();
        while(count<=100)
        {
            int DF = pq.remove();
            try
            {
                Integer.parseInt(map.get(DF).get(0));
            }
            catch(Exception e)
            {
                answer.add(map.get(DF).get(0));
                System.out.print("\"" + map.get(DF).get(0) + "\",");
                df.add(DF);
                count++;
            }
            map.get(DF).remove(0);
        }
        System.out.println();
        for(int DF : df)
        {
            System.out.print(DF + ",");
        }
    }
}