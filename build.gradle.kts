// Root project has no source code.
// All shared build logic is in build-logic/ convention plugins.

// This block exists for Dependabot, not for Gradle. The root project declares no dependencies and
// applies no plugins, so it resolves nothing and the repositories below are inert at build time;
// modules get theirs from scalardb-saga.java-conventions.
//
// Dependabot builds its repository list from the top-level build file and the root settings file
// only. It never reads build-logic's own repositories block, even for dependencies declared there,
// and when it finds no repositories at all it falls back to Maven Central alone. The Error Prone,
// NullAway, SpotBugs and license-report Gradle plugins publish solely to the Gradle Plugin Portal,
// so on a Central-only view they look nonexistent and stay pinned forever. Deleting this block
// re-freezes them, and nothing reports the gap: the update simply never arrives.
repositories {
    gradlePluginPortal()
    mavenCentral()
}
