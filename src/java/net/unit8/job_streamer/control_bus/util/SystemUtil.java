package net.unit8.job_streamer.control_bus.util;

import clojure.lang.*;

/**
 * @author Yuki Seki
 */
public class SystemUtil {
    public static Object getSystem() {
        Var varSystem = RT.var("job-streamer.control-bus.main", "system");
        if (varSystem.isBound()) {
            Atom mainSystemAtom = (Atom) varSystem.get();
            Object system = mainSystemAtom.deref();
            if (system != null) {
                return system;
            }
        }

        // Fallback... System must be started in REPL.
        return RT.var("reloaded.repl", "system").get();
    }
}

