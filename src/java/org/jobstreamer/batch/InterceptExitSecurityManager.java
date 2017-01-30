package org.jobstreamer.batch;

import java.security.Permission;

/**
 * @author kawasima
 */
public class InterceptExitSecurityManager extends SecurityManager {
    private Integer exitStatus;

    @Override
    public void checkExit(int status) {
        exitStatus = status;
        throw new SecurityException("System.exit called");
    }

    @Override
    public void checkPermission(final Permission perm) {
    }

    @Override
    public void checkPermission(final Permission perm, final Object context) {
    }

    public int getExitStatus() {
        return exitStatus;
    }
}
