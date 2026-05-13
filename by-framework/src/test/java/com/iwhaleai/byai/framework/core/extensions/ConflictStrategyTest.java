package com.iwhaleai.byai.framework.core.extensions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ConflictStrategy enum.
 */
class ConflictStrategyTest {

    @Test
    void conflictStrategiesHaveCorrectValues() {
        assertEquals("error", ConflictStrategy.ERROR.getValue());
        assertEquals("overwrite", ConflictStrategy.OVERWRITE.getValue());
        assertEquals("skip", ConflictStrategy.SKIP.getValue());
    }

    @Test
    void conflictStrategiesCount() {
        assertEquals(3, ConflictStrategy.values().length);
    }

    @Test
    void conflictStrategiesCanBeFoundByName() {
        assertEquals(ConflictStrategy.ERROR, ConflictStrategy.valueOf("ERROR"));
        assertEquals(ConflictStrategy.OVERWRITE, ConflictStrategy.valueOf("OVERWRITE"));
        assertEquals(ConflictStrategy.SKIP, ConflictStrategy.valueOf("SKIP"));
    }
}