/**
 * 
 */
package org.jocean.event.api;

import org.jocean.event.api.internal.Eventable;
import org.jocean.idiom.ArgsHandler;
import org.jocean.idiom.ReferenceCounted;

/**
 * @author isdom
 *
 */
public class RefcountedGuardEventable extends ArgsHandler.Consts.PairedArgsGuard implements Eventable {

    public RefcountedGuardEventable(final String event) {
        super(ReferenceCounted.Utils._REFCOUNTED_GUARD);
        this._event = event;
    }
    
    @Override
    public String event() {
        return this._event;
    }

    private final String _event;
}
