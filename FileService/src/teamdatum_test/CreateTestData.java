package teamdatum_test;

import java.io.File;
import java.io.FileWriter;

/**
 * Created by gwo on 4/1/14.
 */
public class CreateTestData {

    public static void main(String[] args)
        throws Exception
    {
        String rec = "aaaaa,bbbbb,ccccc,ddddd,eeeee,fffff,ggggg,hhhhh,iiiii,jjjjj,kkkkk,lllll,mmmmm,nnnnn,ooooo,ppppp,qqqqq,rrrrr,sssss,ttttt,uuuuu,vvvvv,wwwww,xxxxx,yyyyy,zzzzz\n";
        File f = new File("test_data.txt");
        FileWriter fw = new FileWriter(f);
        for (int i = 0; i < 9699968; i++) {
            fw.write(rec);
        }
        fw.close();
    }

}
