/**
 * 
 */
package org.jocean.event.api.internal;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

import org.jocean.event.api.annotation.OnEvent;
import org.jocean.idiom.ReflectUtils;


/**
 * @author isdom
 *
 */
public class DefaultInvoker implements EventInvoker {
    public static EventInvoker[] invokers(final Object target) {
        return invokers(target, OnEvent.class, null);
    }
    
    public static EventInvoker[] invokers(final Object target, final String suffix) {
        return invokers(target, OnEvent.class, suffix);
    }
    
    public static EventInvoker[] invokers(
            final Object target, 
            final Class<? extends Annotation> anno, 
            final String suffix) {
        if ( null == target ) {
            return null;
        }
        
        final Method[] methods = ReflectUtils.getAnnotationMethodsOf(target.getClass(), anno);
        if ( methods.length <= 0 ) {
            return null;
        }
        return (new ArrayList<EventInvoker>() { 
            private static final long serialVersionUID = 1L;
        {
            for ( int idx = 0; idx < methods.length; idx++) {
                this.add(DefaultInvoker.of(target, methods[idx], suffix));
            }
        } }).toArray(new EventInvoker[0]);
    }
    
    public static DefaultInvoker of(final Object target, final String methodName) {
        return of(target, methodName, null);
    }
    
    public static DefaultInvoker of(final Object target, final Method method) {
        return of(target, method, null);
    }
    
    public static DefaultInvoker of(final Object target, final String methodName, 
            final String suffix) {
        if ( null == target ) {
            return null;
        }
        final Method[] methods = target.getClass().getDeclaredMethods();
        for ( Method method : methods ) {
            if ( method.getName().equals(methodName) ) {
                return new DefaultInvoker(target, method, suffix);
            }
        }
        return null;
    }
    
    public static DefaultInvoker of(final Object target, final Method method, 
            final String suffix) {
        if ( null == target || null == method) {
            return null;
        }
        return new DefaultInvoker(target, method, suffix);
    }
    
	private DefaultInvoker(final Object target, final Method method, 
	        final String suffix) {
		this._target = target;
		this._method = method;
		this._method.setAccessible(true);
		this._event = concat(eventAnnotationByMethod(method), suffix);
	}
	
	private static String concat(final String s1, final String s2) {
	    if (null==s1) {
	        return s2;
	    } else if (null==s2) {
	        return s1;
	    } else {
	        return s1 + s2;
	    }
	}
	
    @SuppressWarnings("unchecked")
	@Override
	public <RET> RET invoke(final Object[] args) throws Throwable {
        try {
            return (RET)this._method.invoke(this._target, args);
        }
        catch (final InvocationTargetException invocationTargetException) {
            throw invocationTargetException.getCause();
        }
	}
	
	@Override
	public String getBindedEvent() {
	    return this._event;
	}
	
    private static String eventAnnotationByMethod(final Method method) {
        final OnEvent onEvent = method.getAnnotation(OnEvent.class);
        return  null != onEvent ? onEvent.event() : null;
    }

	@Override
    public String toString() {
        return "invoker [(" + _target + ")." + _method.getName()
                + "/event(" + _event + ")]";
    }

    private final Object _target;
	private final Method _method;
	private final String _event;
}
