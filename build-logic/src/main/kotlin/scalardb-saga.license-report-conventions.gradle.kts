import com.github.jk1.license.render.TextReportRenderer

plugins {
    id("com.github.jk1.dependency-license-report")
}

// Generates THIRD-PARTY-NOTICES.txt for a module that redistributes its dependencies — currently only
// :daemon, whose container image vendors ~230 third-party jars whose licenses and copyright notices
// have to travel with it. Derived from the resolved runtime classpath rather than maintained by hand,
// so it cannot drift from what actually ships.
//
// This lives in build-logic rather than in the consuming build script for a mechanical reason:
// declaring an external plugin in a subproject's own `plugins {}` block gives that project a separate
// classloader scope from its siblings, and Spotless' shared build service then fails to load ("Cannot
// set the value of task ':daemon:spotlessJava' property 'taskService'"). Routing every external plugin
// through build-logic keeps one scope for the whole build. The same reasoning is why :rpc's protobuf
// setup is a convention plugin despite having a single consumer.

licenseReport {
    configurations = arrayOf("runtimeClasspath")
    renderers = arrayOf(TextReportRenderer("THIRD-PARTY-NOTICES.txt"))
}

// The plugin holds a Project reference in its task, which the configuration cache cannot serialize.
// Declaring the incompatibility makes Gradle disable the cache for builds that include this task
// instead of reporting a problem and discarding the entry it just wrote. The cost stays confined to
// the image path — `check`, `test`, and `publish` never reach this task, so they keep the cache.
tasks.named("generateLicenseReport") {
    notCompatibleWithConfigurationCache(
        "com.github.jk1.dependency-license-report keeps a Project reference in ReportTask",
    )
}
