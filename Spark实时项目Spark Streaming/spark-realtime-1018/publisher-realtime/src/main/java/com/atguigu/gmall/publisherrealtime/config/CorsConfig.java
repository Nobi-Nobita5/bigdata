package com.atguigu.gmall.publisherrealtime.config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;
/**
 * @Author: Xionghx
 * @Date: 2023/02/21/23:09
 * @Version: 1.0
 * 解决跨域问题
 * 后台springboot配置允许跨域访问
 */
@Configuration
public class CorsConfig {
    private CorsConfiguration buildConfig() {
        CorsConfiguration corsConfiguration = new CorsConfiguration();

        corsConfiguration.addAllowedOrigin("*"); //允许任何域名

        corsConfiguration.addAllowedHeader("*"); //允许任何头

        corsConfiguration.addAllowedMethod("*"); //允许任何方法

        return corsConfiguration;

    }

    @Bean

    public CorsFilter corsFilter() {
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();

        source.registerCorsConfiguration("/**", buildConfig()); //注册

        return new CorsFilter(source);

    }

}
