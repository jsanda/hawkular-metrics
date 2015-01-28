/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rhq.metrics.clients.ptrans;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.assertj.core.api.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestWatcher;
import org.junit.rules.Timeout;
import org.junit.runner.Description;

/**
 * @author Thomas Segismont
 */
public class MainITest {
    private static String JAVA;
    private static String PTRANS_ALL;

    @Rule
    public final TestWatcher testWatcher = new PrintOutputOnFailureWatcher();
    @Rule
    public final Timeout timeout = new Timeout((int) MILLISECONDS.convert(20, SECONDS));
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private File ptransConfFile;
    private File ptransOut;
    private File ptransErr;
    private File pTransPidFile;
    private ProcessBuilder pTransProcessBuilder;
    private Process pTransProcess;

    @BeforeClass
    public static void beforeClass() {
        File javaHome = new File(System.getProperty("java.home"));
        File javaBin = new File(javaHome, "bin");
        JAVA = new File(javaBin, "java").getAbsolutePath();

        PTRANS_ALL = new File(System.getProperty("ptrans-all.path", "target/ptrans-all.jar")).getAbsolutePath();
    }

    @Before
    public void before() throws Exception {
        ptransConfFile = temporaryFolder.newFile();
        try (FileOutputStream out = new FileOutputStream(ptransConfFile)) {
            Resources.copy(Resources.getResource("ptrans.conf"), out);
        }

        ptransOut = temporaryFolder.newFile();
        ptransErr = temporaryFolder.newFile();

        pTransPidFile = temporaryFolder.newFile();

        pTransProcessBuilder = new ProcessBuilder();
        pTransProcessBuilder.directory(temporaryFolder.getRoot());
        pTransProcessBuilder.redirectOutput(ptransOut);
        pTransProcessBuilder.redirectError(ptransErr);

        pTransProcessBuilder.command(JAVA, "-jar", PTRANS_ALL);
    }

    @Test
    public void shouldExitWithErrorIfConfigPathIsMissing() throws Exception {
        pTransProcess = pTransProcessBuilder.start();
        int returnCode = pTransProcess.waitFor();
        assertThat(returnCode).isNotEqualTo(0);

        assertThat(ptransErr).isFile().canRead().hasContent("Missing required option: c");
    }

    @Test
    public void shouldExitWithHelpIfOptionIsPresent() throws Exception {
        pTransProcessBuilder.command().add("-h");

        pTransProcess = pTransProcessBuilder.start();
        int returnCode = pTransProcess.waitFor();
        assertThat(returnCode).isEqualTo(0);

        assertThat(ptransErr).isFile().canRead().hasContent("");
    }

    @Test
    public void shouldWritePidToPidFile() throws Exception {
        pTransProcessBuilder.command().addAll(
                Lists.newArrayList(
                        "-c", ptransConfFile.getAbsolutePath(),
                        "-p", pTransPidFile.getAbsolutePath()
                )
        );

        pTransProcess = pTransProcessBuilder.start();
        boolean isRunning = pTransProcess.isAlive();
        for (int i = 0; !isRunning && i < 5; i++) {
            isRunning = !pTransProcess.waitFor(1, SECONDS);
        }
        assertThat(isRunning).isTrue();

        boolean pidFileWritten = pTransPidFile.canRead() && pTransPidFile.length() > 0;
        for (int i = 0; !pidFileWritten && i < 5; i++) {
            Thread.sleep(MILLISECONDS.convert(1, SECONDS));
            pidFileWritten = pTransPidFile.canRead() && pTransPidFile.length() > 0;
        }
        assertThat(pTransPidFile).isFile().canRead().is(writeLocked());

        if (pTransProcess.getClass().getName().equals("java.lang.UNIXProcess")) {
            Field pidField = pTransProcess.getClass().getDeclaredField("pid");
            pidField.setAccessible(true);
            int pid = pidField.getInt(pTransProcess);
            assertThat(pTransPidFile).hasContent(String.valueOf(pid));
        }
    }

    @After
    public void after() {
        if (pTransProcess != null && pTransProcess.isAlive()) {
            pTransProcess.destroy();
        }
    }

    private Condition<? super File> writeLocked() {
        return new Condition<File>("write-locked") {
            @Override
            public boolean matches(File file) {
                try (
                        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                        FileChannel channel = randomAccessFile.getChannel();
                        FileLock fileLock = channel.tryLock()
                ) {
                    return fileLock == null;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private class PrintOutputOnFailureWatcher extends TestWatcher {
        private static final String SEPARATOR = "###########";

        @Override
        protected void failed(Throwable e, Description description) {
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            printWriter.printf("%s, pTrans output%n", description.getMethodName());
            printWriter.println(SEPARATOR);
            try {
                Files.readAllLines(ptransOut.toPath()).forEach(printWriter::println);
            } catch (IOException ignored) {
            }
            printWriter.println(SEPARATOR);
            printWriter.printf("%s, pTrans err%n", description.getMethodName());
            printWriter.println(SEPARATOR);
            try {
                Files.readAllLines(ptransErr.toPath()).forEach(printWriter::println);
            } catch (IOException ignored) {
            }
            printWriter.println(SEPARATOR);
            printWriter.close();
            System.out.println(stringWriter.toString());
        }
    }
}