package org.czx.aop;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.czx.impl.ShardContext;
import org.czx.impl.ShardingContextHolder;
import org.czx.vo.RwRole;

import java.lang.reflect.Method;
import java.util.Optional;

@Aspect
@Slf4j
public class ShardingAspect {
    public ShardingAspect(){

    }

    @Pointcut("@annotation(org.czx.aop.WfAnnotation)")
    public void target(){

    }

    @Around("target()")
    public Object shardingAround(ProceedingJoinPoint jp){
        try{
            Optional<WfAnnotation> optional = getAnnotation(jp);
            if(optional.isPresent()){
                WfAnnotation wfa = optional.get();
                ShardContext shardContext = ShardingContextHolder.create();
                shardContext.setLocalTxnCheck(wfa.isCheckLocalTxn());
                if(RwRole.Salve.name().equals(wfa.rwRole())){
                    shardContext.setRwRole(RwRole.Salve);
                }
            }
            Object value = jp.proceed();
            return value;
        }catch (Throwable t){
            throw new RuntimeException(t);
        }finally {
            ShardingContextHolder.clear();
        }
    }

    private static Optional<WfAnnotation> getAnnotation(ProceedingJoinPoint jp){
        if(jp.getSignature() instanceof MethodSignature) {
            MethodSignature signature = (MethodSignature) jp.getSignature();
            Method function = signature.getMethod();
            WfAnnotation annotation = function.getAnnotation(WfAnnotation.class);
            if(annotation != null){
                log.info("Get annotation:master={},checkLocalTxn={}", annotation.rwRole(),
                        annotation.isCheckLocalTxn());
                return Optional.of(annotation);
            }
        }
        return Optional.empty();
    }

}
