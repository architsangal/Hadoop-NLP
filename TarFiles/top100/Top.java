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
        while(count<=100)
        {
            int DF = pq.remove();
            answer.add(map.get(DF).get(0));
            map.get(DF).remove(0);
            count++;
        }

        System.out.println(answer);
    }
}