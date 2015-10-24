/**
 * 
 */
package org.jocean.event.api;

import org.jocean.event.api.internal.Eventable;

/**
 * @author isdom
 *
 */
public abstract class AbstractUnhandleAware implements EventUnhandleAware, Eventable {

    public AbstractUnhandleAware(final String event) {
        this._event = event;
    }
    
    @Override
    public String event() {
        return this._event;
    }

    private final String _event;
}
