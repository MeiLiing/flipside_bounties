with deposited_lp as (
  select
    event_attributes:sender as provider,
    event_attributes:"0_contract_address" as lp_address,
    sum(event_attributes:share) as provided_lp_tokens
  from terra.msg_events
where lp_address in ('terra1yjg0tuhc6kzwz9jl8yqgxnf2ctwlfumnvscupp','terra106a00unep7pvwvcck4wylt4fffjhgkf9a0u6eu','terra1sgu6yca6yjk0a34l86u6ju4apjcd6refwuhgzv','terra17pzt8t2hmx6587zn6yh5ensylm3s9mm4m72v2n','terra1dw5j23l6nwge69z0enemutfmyc93c36aqnzjj5','terra1jfp5ew4tsru98wthajsqegtzaxt49ty4z2qws0','terra1jkr0ef9fpghdru38ht70ds6jfldprgttw6xlek','terra1cpzkckgzz90pq8fkumdjc58ee5llrxt2yka9fp','terra1lgazu0ltsxm3ayellqa2mhnhlvgx3hevkqeqy2','terra13eggta6zqfg03mxgqg9p5paqka7tgaaxnkhuuu','terra1f6d3pggq7h2y7jrgwxp3xh08yhvj8znalql87h','terra15568nqrqcawm263yqcuuuvj5mh763tp8jyscq3','terra1whns5nyc8sw328uw3qqnyafxd5yfqsytkdkgqz','terra178yhudw6cwtnrn4fq593z87y385m0xe9n6x423','terra1wh2jqjkagzyd3yl4sddlapy45ry808xe80fchh','terra17e0aslpj3rrt62gwh7utj3fayas4h8dl3y8ju3','terra17kmgn775v2y9m35gunn3pnw8s2ggv6h95wvkxr','terra1fgc8tmys2kxzyl39k3gtgw9dlxxuhlqux7k38e','terra1pmmfz205es7axymwpl4jj2ruxnevufu3462f02','terra1v93ll6kqp33unukuwls3pslquehnazudu653au','terra18r6rdnkgrg74zew3d8l9nhk0m4xanpeukw3e20','terra1me6a35xuzf9ycjaaqyj7798g5tewunp6dzg27e','terra123neekasfmvcs4wa70cgw3j3uvwzqacdz2we03','terra15xxfkxldxse9atz8ce7ne2a047pp09dy428njm','terra16j5f4lp4z8dddm3rhyw8stwrktyhcsc8ll6xtt','terra1l60336rkawujnwk7lgfq5u0s684r99p3y8hx65','terra1x4msd26x8ncqcng3dapx8306rmgwuwtlg30a2j','terra154jt8ppucvvakvqa5fyfjdflsu6v83j4ckjfq3','terra1tus5ec9qsdht8dapq9ldfnsf8eehnfmwvsut83','terra1kw95x0l3qw5y7jw3j0h3fy83y56rj68wd8w7rc','terra13ay3hftcft25uazl76q8gmdk993y9nyv9avu2h','terra1w7hny2catfwsv6dq8gfm4zgazx3hmpl3xwzxya','terra1a26j00ywq0llvms707hqycwlkl9erwhacr6jve','terra1tksv8cn7j95mecxjhj3dgcz3xtylcw5xkarre7','terra10s94a5gesvayqlekgn570r3nsnmr8q7lf5zkjp','terra12aazc56hv7aj2fcvmhuxve0l4pmayhpn794m0p','terra1efvhm927dehrka0cgpcptt5gvjfdgqm07smawu','terra13f87x4c87ct5545t3j4mqw4k6jmgds5609z92c')
  and event_type = 'from_contract'
    and event_attributes:"0_action" = 'provide_liquidity'
    and provider not in ('terra1l4smc62umdn2yjh202m2e8pgas2gx400a0l5hv') -- Loop Instantiator
    and tx_status = 'SUCCEEDED'
  group by 1,2
),
withdrawn_lp as (
  select
    event_attributes:sender as provider,
    event_attributes:"1_contract_address" as lp_address,
    sum(event_attributes:withdrawn_share) as withdrawn_lp_tokens
  from terra.msg_events
  where lp_address in ('terra1yjg0tuhc6kzwz9jl8yqgxnf2ctwlfumnvscupp','terra106a00unep7pvwvcck4wylt4fffjhgkf9a0u6eu','terra1sgu6yca6yjk0a34l86u6ju4apjcd6refwuhgzv','terra17pzt8t2hmx6587zn6yh5ensylm3s9mm4m72v2n','terra1dw5j23l6nwge69z0enemutfmyc93c36aqnzjj5','terra1jfp5ew4tsru98wthajsqegtzaxt49ty4z2qws0','terra1jkr0ef9fpghdru38ht70ds6jfldprgttw6xlek','terra1cpzkckgzz90pq8fkumdjc58ee5llrxt2yka9fp','terra1lgazu0ltsxm3ayellqa2mhnhlvgx3hevkqeqy2','terra13eggta6zqfg03mxgqg9p5paqka7tgaaxnkhuuu','terra1f6d3pggq7h2y7jrgwxp3xh08yhvj8znalql87h','terra15568nqrqcawm263yqcuuuvj5mh763tp8jyscq3','terra1whns5nyc8sw328uw3qqnyafxd5yfqsytkdkgqz','terra178yhudw6cwtnrn4fq593z87y385m0xe9n6x423','terra1wh2jqjkagzyd3yl4sddlapy45ry808xe80fchh','terra17e0aslpj3rrt62gwh7utj3fayas4h8dl3y8ju3','terra17kmgn775v2y9m35gunn3pnw8s2ggv6h95wvkxr','terra1fgc8tmys2kxzyl39k3gtgw9dlxxuhlqux7k38e','terra1pmmfz205es7axymwpl4jj2ruxnevufu3462f02','terra1v93ll6kqp33unukuwls3pslquehnazudu653au','terra18r6rdnkgrg74zew3d8l9nhk0m4xanpeukw3e20','terra1me6a35xuzf9ycjaaqyj7798g5tewunp6dzg27e','terra123neekasfmvcs4wa70cgw3j3uvwzqacdz2we03','terra15xxfkxldxse9atz8ce7ne2a047pp09dy428njm','terra16j5f4lp4z8dddm3rhyw8stwrktyhcsc8ll6xtt','terra1l60336rkawujnwk7lgfq5u0s684r99p3y8hx65','terra1x4msd26x8ncqcng3dapx8306rmgwuwtlg30a2j','terra154jt8ppucvvakvqa5fyfjdflsu6v83j4ckjfq3','terra1tus5ec9qsdht8dapq9ldfnsf8eehnfmwvsut83','terra1kw95x0l3qw5y7jw3j0h3fy83y56rj68wd8w7rc','terra13ay3hftcft25uazl76q8gmdk993y9nyv9avu2h','terra1w7hny2catfwsv6dq8gfm4zgazx3hmpl3xwzxya','terra1a26j00ywq0llvms707hqycwlkl9erwhacr6jve','terra1tksv8cn7j95mecxjhj3dgcz3xtylcw5xkarre7','terra10s94a5gesvayqlekgn570r3nsnmr8q7lf5zkjp','terra12aazc56hv7aj2fcvmhuxve0l4pmayhpn794m0p','terra1efvhm927dehrka0cgpcptt5gvjfdgqm07smawu','terra13f87x4c87ct5545t3j4mqw4k6jmgds5609z92c')
    and event_type = 'from_contract'
    and event_attributes:"1_action" = 'withdraw_liquidity'
    and provider not in ('terra1l4smc62umdn2yjh202m2e8pgas2gx400a0l5hv') -- Loop Instantiator
    and tx_status = 'SUCCEEDED'
  group by 1,2
),
provider_balance as (
  select 
    deposited_lp.provider,
    case  
      when deposited_lp.lp_address = 'terra106a00unep7pvwvcck4wylt4fffjhgkf9a0u6eu' then 'loop-ust'
      when deposited_lp.lp_address = 'terra1sgu6yca6yjk0a34l86u6ju4apjcd6refwuhgzv' then 'luna-ust'
      when deposited_lp.lp_address = 'terra1yjg0tuhc6kzwz9jl8yqgxnf2ctwlfumnvscupp' then 'halo-ust'
      when deposited_lp.lp_address = 'terra1dw5j23l6nwge69z0enemutfmyc93c36aqnzjj5' then 'loopr-ust'
      when deposited_lp.lp_address = 'terra1jfp5ew4tsru98wthajsqegtzaxt49ty4z2qws0' then 'twd-ust'
      when deposited_lp.lp_address = 'terra1jkr0ef9fpghdru38ht70ds6jfldprgttw6xlek' then 'spec-ust'
      when deposited_lp.lp_address = 'terra1cpzkckgzz90pq8fkumdjc58ee5llrxt2yka9fp' then 'mir-ust'
      when deposited_lp.lp_address = 'terra1lgazu0ltsxm3ayellqa2mhnhlvgx3hevkqeqy2' then 'stt-ust'
      when deposited_lp.lp_address = 'terra13eggta6zqfg03mxgqg9p5paqka7tgaaxnkhuuu' then 'mine-ust'
      when deposited_lp.lp_address = 'terra1f6d3pggq7h2y7jrgwxp3xh08yhvj8znalql87h' then 'anc-ust'
      when deposited_lp.lp_address = 'terra15568nqrqcawm263yqcuuuvj5mh763tp8jyscq3' then 'lota-ust'
      when deposited_lp.lp_address = 'terra1whns5nyc8sw328uw3qqnyafxd5yfqsytkdkgqz' then 'alte-ust'
      when deposited_lp.lp_address = 'terra178yhudw6cwtnrn4fq593z87y385m0xe9n6x423' then 'dph-ust'
      when deposited_lp.lp_address = 'terra1wh2jqjkagzyd3yl4sddlapy45ry808xe80fchh' then 'kuji-ust'
      when deposited_lp.lp_address = 'terra17e0aslpj3rrt62gwh7utj3fayas4h8dl3y8ju3' then 'wewbtc-ust'
      when deposited_lp.lp_address = 'terra17kmgn775v2y9m35gunn3pnw8s2ggv6h95wvkxr' then 'weweth-ust'
      when deposited_lp.lp_address = 'terra1fgc8tmys2kxzyl39k3gtgw9dlxxuhlqux7k38e' then 'wewbtc-luna'
      when deposited_lp.lp_address = 'terra1pmmfz205es7axymwpl4jj2ruxnevufu3462f02' then 'weweth-luna'
      when deposited_lp.lp_address = 'terra1v93ll6kqp33unukuwls3pslquehnazudu653au' then 'bluna-luna'
      when deposited_lp.lp_address = 'terra18r6rdnkgrg74zew3d8l9nhk0m4xanpeukw3e20' then 'bluna-ust'
      when deposited_lp.lp_address = 'terra1me6a35xuzf9ycjaaqyj7798g5tewunp6dzg27e' then 'whmim-ust'
      when deposited_lp.lp_address = 'terra123neekasfmvcs4wa70cgw3j3uvwzqacdz2we03' then 'aust-ust'
      when deposited_lp.lp_address = 'terra15xxfkxldxse9atz8ce7ne2a047pp09dy428njm' then 'anc-aust'
      when deposited_lp.lp_address = 'terra16j5f4lp4z8dddm3rhyw8stwrktyhcsc8ll6xtt' then 'luna-aust'
      when deposited_lp.lp_address = 'terra1l60336rkawujnwk7lgfq5u0s684r99p3y8hx65' then 'kuji-aust'
      when deposited_lp.lp_address = 'terra1x4msd26x8ncqcng3dapx8306rmgwuwtlg30a2j' then 'loop-weweth'
      when deposited_lp.lp_address = 'terra154jt8ppucvvakvqa5fyfjdflsu6v83j4ckjfq3' then 'loop-loopr'
      when deposited_lp.lp_address = 'terra1kw95x0l3qw5y7jw3j0h3fy83y56rj68wd8w7rc' then 'loop-lota'
      when deposited_lp.lp_address = 'terra13ay3hftcft25uazl76q8gmdk993y9nyv9avu2h' then 'loop-mir'
      when deposited_lp.lp_address = 'terra1w7hny2catfwsv6dq8gfm4zgazx3hmpl3xwzxya' then 'loop-anc'
      when deposited_lp.lp_address = 'terra1a26j00ywq0llvms707hqycwlkl9erwhacr6jve' then 'loop-mine'
      when deposited_lp.lp_address = 'terra1tksv8cn7j95mecxjhj3dgcz3xtylcw5xkarre7' then 'loop-wewbtc'
      when deposited_lp.lp_address = 'terra1tus5ec9qsdht8dapq9ldfnsf8eehnfmwvsut83' then 'loop-luna'
      when deposited_lp.lp_address = 'terra10s94a5gesvayqlekgn570r3nsnmr8q7lf5zkjp' then 'loop-whmim'
      when deposited_lp.lp_address = 'terra12aazc56hv7aj2fcvmhuxve0l4pmayhpn794m0p' then 'loop-halo'
      when deposited_lp.lp_address = 'terra1efvhm927dehrka0cgpcptt5gvjfdgqm07smawu' then 'loop-aust'
      when deposited_lp.lp_address = 'terra13f87x4c87ct5545t3j4mqw4k6jmgds5609z92c' then 'loop-kuji'
      else null 
    end as lp,
    provided_lp_tokens,
    ifnull(withdrawn_lp_tokens,0) as withdrawn_lp_tokens,
    provided_lp_tokens - ifnull(withdrawn_lp_tokens,0) as balance
  from deposited_lp
  left join withdrawn_lp
    on deposited_lp.provider = withdrawn_lp.provider
      and deposited_lp.lp_address = withdrawn_lp.lp_address
)
select *
from provider_balance
where balance > 0