package com.yr.kudu.arithmetic;

/**
 * @author baiyang
 * @version 1.0
 * @date 2020/5/12 3:54 下午
 */
public class KMPArithmetic {

    public static void GetNextval(String keyword, int nextval[]) {

        char[] chars = keyword.toCharArray();
        int p_len = chars.length -1;
        // keyword 的下标
        int i = -1;
        int j = -1;
        nextval[0] = -1;

        while (i < p_len) {
            if (j == -1 || chars[i] == chars[j]) {
                i++;
                j++;

                if (chars[i] != chars[j]) {
                    nextval[i] = j;
                } else {
                    // 既然相同就继续往前找真前缀
                    nextval[i] = nextval[j];
                }

            } else {
                j = nextval[j];
            }
        }
    }

    public static int kmp(String source, String keyword, int next[]){
        GetNextval(keyword,next);
        // source 的下标
        int i = 0;
        // keyword 的下标
        int j = 0;
        char[] sourceCharArray = source.toCharArray();
        char[] keywordCharArray = keyword.toCharArray();
        int s_len = sourceCharArray.length;
        int p_len = keywordCharArray.length;
        // 因为末尾 '\0' 的存在，所以不会越界
        while (i < s_len && j < p_len)
        {
            // keyword 的第一个字符不匹配或 sourceCharArray[i] == keywordCharArray[j]
            if (j == -1 || sourceCharArray[i] == keywordCharArray[j])
            {
                i++;
                j++;
            }
            else{
                // 当前字符匹配失败，进行跳转
                j = next[j];
            }
        }

        if (j == p_len){
            // 匹配成功
            return i - j;
        }
        return -1;
    }
}
