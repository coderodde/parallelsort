package net.coderodde.util;

final class LongBucketSorterInputTask {

    final long[] sourceArray;
    final long[] targetArray;
    final int threads;
    final int recursionDepth;
    final int sourceArrawFromIndex;
    final int sourceArrayToIndex;
    final int targetArrayFromIndex;

    LongBucketSorterInputTask(final long[] sourceArray,
                           final long[] targetArray,
                           final int threads,
                           final int recursionDepth,
                           final int sourceArrayFromIndex,
                           final int sourceArrayToIndex,
                           final int targetArrayFromIndex) {
        this.sourceArray          = sourceArray;
        this.targetArray          = targetArray;
        this.threads              = threads;
        this.recursionDepth       = recursionDepth;
        this.sourceArrawFromIndex = sourceArrayFromIndex;
        this.sourceArrayToIndex   = sourceArrayToIndex;
        this.targetArrayFromIndex = targetArrayFromIndex;
    }
}
