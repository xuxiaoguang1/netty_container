/**
 * 
 */
package org.jocean.idiom;


/**
 * @author isdom
 *
 */
public interface ExectionLoop {
    public boolean inExectionLoop();
    public Detachable submit(final Runnable runnable);
    public Detachable schedule(final Runnable runnable, final long delayMillis);
    
    public static final ExectionLoop immediateLoop = new ExectionLoop() {

        
        public boolean inExectionLoop() {
            return true;
        }

        
        public Detachable submit(final Runnable runnable) {
            runnable.run();
            return new Detachable() {
                
                public void detach() {
                }};
        }

        
        public Detachable schedule(final Runnable runnable, final long delayMillis) {
            runnable.run();
            return new Detachable() {
                
                public void detach() {
                }};
        }};
}
