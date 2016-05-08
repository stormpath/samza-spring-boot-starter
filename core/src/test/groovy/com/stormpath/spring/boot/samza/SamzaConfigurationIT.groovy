package com.stormpath.spring.boot.samza

import org.apache.samza.container.SamzaContainer
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.SpringApplicationConfiguration
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner

import static org.junit.Assert.assertNotNull

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes=[SamzaAutoConfiguration, TestTaskConfiguration])
@TestPropertySource("classpath:com/stormpath/spring/boot/samza/application.properties")
class SamzaConfigurationIT {

    @Autowired
    private SamzaContainer samzaContainer;

    @Test
    void testThreadJob() {
        assertNotNull samzaContainer
    }
}
