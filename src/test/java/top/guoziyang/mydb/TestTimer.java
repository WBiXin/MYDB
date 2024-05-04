package top.guoziyang.mydb;

/**
 * ClassName: TestTimer
 * Description:
 *
 * @Author WangBX
 * @Create 2024/4/20 19:26
 * @Version 1.0
 */
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;

public class TestTimer{
    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        int n = in.nextInt();
        int k = in.nextInt();
        int ans = 0;
        int[] nums = new int[n];
        int[] sum = new int[n];
        int[] dp = new int[n];
        int[] f_dp = new int[n];
        for(int i = 0;i < n;i++){
            nums[i] = in.nextInt();
            if(i == 0)
                sum[0] = nums[i];
            else
                sum[i] = sum[i - 1] + nums[i];
        }
        for(int i = 0;i < n;i++){
            if(i < n-1 && sum[i] - sum[0] + nums[0] >= k && sum[n-1] - sum[i] >= k){
                f_dp[i] = 1;
            }
            else{
                f_dp[i] = 0;
            }
        }
        if(sum[n - 1] < k){
            System.out.println(0);
            return;
        }
        if(sum[n - 1] >= k){
            dp[n - 1] = 1;
        }
        else{
            dp[n - 1] = 0;
        }
        for(int i = n - 2;i >= 0;i--){
            if(sum[i] >= k && sum[n-1] - sum[i] >= k){
                dp[i] = 1 + dp[i + 1];
            }
        }
        ans = dp[0];
        //ans = split(nums,sum,k,0,n-1);
        ans %= 1000000007;
        System.out.println(ans);
    }

    private static int split(int[] nums,int[] sum,int k,int start,int end){
        int ans = 0;
        for(int i = start;i <= end;i++){
            if(i < end && sum[i] - sum[start] + nums[start] >= k && sum[end] - sum[i] >= k){
                ans = ans + split(nums,sum,k,i+1,end);
            }
            else if(i == end && sum[i] - sum[start] + nums[start] >= k){
                ans += 1;
            }
        }
        return ans;
    }
}