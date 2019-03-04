package net.coderodde.util.support;

public final class LongBucketSorterInputTask {

   public final long[] sourceArray;
   public final long[] targetArray;
   public final int threads;
   public final int recursionDepth;
   public final int auxiliaryBufferOffset;
   public final int sourceArrawFromIndex;
   public final int sourceArrayToIndex;
   public final int targetArrayFromIndex;

    public LongBucketSorterInputTask(final long[] sourceArray,
                                     final long[] targetArray,
                                     final int threads,
                                     final int recursionDepth,
                                     final int auxiliaryBufferOfffset,
                                     final int sourceArrayFromIndex,
                                     final int sourceArrayToIndex,
                                     final int targetArrayFromIndex) {
        this.sourceArray           = sourceArray;
        this.targetArray           = targetArray;
        this.threads               = threads;
        this.recursionDepth        = recursionDepth;
        this.auxiliaryBufferOffset = auxiliaryBufferOfffset;
        this.sourceArrawFromIndex  = sourceArrayFromIndex;
        this.sourceArrayToIndex    = sourceArrayToIndex;
        this.targetArrayFromIndex  = targetArrayFromIndex;
    }
}
