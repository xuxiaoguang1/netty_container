/**
 * 
 */
package org.jocean.idiom.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author isdom
 *
 */
public class CachedBytesPool extends AbstractCachedObjectPool<byte[]> 
    implements BytesPool, CachedObjectPool<byte[]> {

    private final static Logger LOG = LoggerFactory.getLogger(CachedBytesPool.class);

    public CachedBytesPool(final int blockSize) {
        super(LOG);
        if ( blockSize <= 0 ) {
            throw new IllegalArgumentException("blockSize for CachedBytesPool must more than zero.");
        }
        this._blockSize = blockSize;
    }
    
    
    protected byte[] createObject() {
        return new byte[this._blockSize];
    }

    
    public int getTotalCachedSizeInByte() {
        return this.getCachedCount() * this._blockSize;
    }

    
    public int getTotalRetainedSizeInByte() {
        return this.getRetainedCount() * this._blockSize;
    }
    
    
    public int getTotalSizeInByte() {
        return (this.getCachedCount() + this.getRetainedCount() ) * this._blockSize;
    }
    
    
    public int getBlockSize() {
        return this._blockSize;
    }
    
    private final int _blockSize;
}
