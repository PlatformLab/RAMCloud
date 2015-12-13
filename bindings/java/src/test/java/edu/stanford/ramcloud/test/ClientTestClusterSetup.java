/* Copyright (c) 2014 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package edu.stanford.ramcloud.test;

import edu.stanford.ramcloud.*;

import java.lang.reflect.*;
import org.testng.annotations.*;

/**
 * Class that holds methods to create and destroy the test cluster
 * used for the unit tests.
 */
public class ClientTestClusterSetup {
    public static RAMCloud ramcloud;
    public static TestCluster cluster;

    @BeforeSuite
    public void setupClient() {
        cluster = new TestCluster();
        ClientTestClusterSetup.ramcloud =
                new RAMCloud(cluster.getRamcloudClientPointer());
    }

    @AfterSuite
    public void cleanUpClient() {
        cluster.destroy();
    }

    /**
     * Invoke the specified method on the target object through reflection.
     *
     * @param object
     *      The object to invoke the method on.
     * @param name
     *      The name of the method to invoke.
     * @oaran argClasses
     *      The classes of the arguments to the method
     * @param args
     *      The arguments to invoke with.
     * @return The result of the invoked method.
     */
    public static Object invoke(Object object, String name, Class[] argClasses,
                                Object... args) {
        Object out = null;
        try {
            Method method = object.getClass().getDeclaredMethod(name, argClasses);
            method.setAccessible(true);
            out = method.invoke(object, args);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return out;
    }
}
