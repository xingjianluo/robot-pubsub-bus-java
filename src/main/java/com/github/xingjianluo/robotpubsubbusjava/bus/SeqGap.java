package com.github.xingjianluo.robotpubsubbusjava.bus;

/**
 * 主题内一段连续的序号缺口：[fromSeq, toSeq]（均含）的消息因背压被丢弃，
 * 客户端可据此判断“这段我丢了”，自行决定补缺或重置。
 */
public record SeqGap(long fromSeq, long toSeq) {

    public SeqGap {
        if (fromSeq <= 0L || toSeq < fromSeq) {
            throw new IllegalArgumentException("非法序号缺口 [" + fromSeq + ", " + toSeq + "]");
        }
    }

    public boolean extendsAfter(long previousTo) {
        return fromSeq == previousTo + 1L;
    }
}
