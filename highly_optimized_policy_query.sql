-- 高度优化的保险查询 - 解决执行计划中的性能瓶颈
-- 优化策略：
-- 1. 避免CTE物化，直接在主查询中使用窗口函数
-- 2. 强制优化器使用更好的JOIN顺序
-- 3. 减少不必要的数据访问
-- 4. 使用STRAIGHT_JOIN控制执行顺序

SELECT STRAIGHT_JOIN
    pp_medical.agreement_no AS '包括親契約証券番号（医療）',
    pp_cancer.agreement_no AS '包括親契約証券番号（がん）',
    p.policy_no AS '証券番号',
    CONCAT(g.goods_name, '_', g.version) AS '保険商品名+保険商品バージョン',
    gp.goods_plan_name AS 'プラン名称',
    NULL AS '初年度申込日',
    policy_dates.effective_date AS '始期日',
    policy_dates.expiry_date AS '満期日',
    NULL AS '分割払保険料（税金、割引抜き）',
    NULL AS '分割払保険料（税金、割引込み）',
    NULL AS '処理中（の履歴数）',
    NULL AS '処理済み（の履歴数）',
    pchannel.channel_user_no AS 'チャネルユーザNo',
    NULL AS '回数',
    p.original_policy_no AS '前契約証券番号',
    NULL AS '前契約_保険期間_始期',
    NULL AS '前契約_保険期間_満期',
    NULL AS '初回有料契約証券番号',
    policy_dates.effective_date AS '照会有料契約_保険期間_始期',
    policy_dates.expiry_date AS '照会有料契約_保険期間_満期',
    NULL AS '補償内容名(1)', NULL AS '保険金額(1)', NULL AS '分割払保険料（税金、割引抜き）(1)',
    NULL AS '補償内容名(2)', NULL AS '保険金額(2)', NULL AS '分割払保険料（税金、割引抜き）(2)',
    NULL AS '補償内容名(3)', NULL AS '保険金額(3)', NULL AS '分割払保険料（税金、割引抜き）(3)',
    pcustomer_holder.full_name AS '(加入者)氏名（漢字）',
    pcustomer_holder.full_name2 AS '(加入者)氏名（カナ）',
    pcustomer_holder.birthday AS '(加入者)生年月日',
    pchannel.channel_user_no AS '(加入者)チャネルユーザNo',
    pce_holder.email AS '(加入者)メールアドレス',
    pcp_holder.phone_no AS '(加入者)電話番号',
    pca_holder.zip_code AS '(加入者)郵便番号',
    CONCAT_WS(' ', pca_holder.address11, pca_holder.address12, pca_holder.address13, 
              pca_holder.address14, pca_holder.address15, pca_holder.address21, 
              pca_holder.address22, pca_holder.address23, pca_holder.address24, 
              pca_holder.address25) AS '(加入者)アドレス',
    pcustomer_insurant.full_name AS '(被保険者)氏名（漢字）',
    pcustomer_insurant.full_name2 AS '(被保険者)氏名（カナ）',
    'R' AS '(被保険者)加入者との関係',
    NULL AS '(被保険者)チャンネルユーザID',
    pcustomer_insurant.birthday AS '(被保険者)生年月日',
    pce_insurant.email AS '(被保険者)メールアドレス'
FROM 
    -- 第一步：从policy表开始，利用goods_id索引
    tmnf_policy.policy p USE INDEX (idx_policy_goods_id_desc)
    
    -- 第二步：强制使用医疗产品的索引JOIN
    INNER JOIN tmnf_policy.policy_product pp_medical USE INDEX (idx_policy_id_and_product_id)
        ON p.id = pp_medical.policy_id 
        AND pp_medical.product_id = 727295836831749
    
    -- 第三步：强制使用癌症产品的索引JOIN
    INNER JOIN tmnf_policy.policy_product pp_cancer USE INDEX (idx_policy_id_and_product_id)
        ON p.id = pp_cancer.policy_id 
        AND pp_cancer.product_id = 727309225050118
    
    -- 第四步：使用子查询获取日期信息，避免CTE物化
    INNER JOIN (
        SELECT 
            pp_sub.policy_id,
            MIN(pp_sub.effective_date) AS effective_date,
            MAX(pp_sub.expiry_date) AS expiry_date
        FROM tmnf_policy.policy_product pp_sub
        WHERE pp_sub.policy_id IN (
            SELECT DISTINCT p_sub.id 
            FROM tmnf_policy.policy p_sub USE INDEX (idx_policy_goods_id_desc)
            WHERE p_sub.goods_id = 727419350695938
            ORDER BY p_sub.id DESC 
            LIMIT 2000  -- 预取稍多一些记录以确保有足够的匹配
        )
        GROUP BY pp_sub.policy_id
    ) policy_dates ON policy_dates.policy_id = p.id
    
    -- 其余LEFT JOIN保持不变但优化顺序
    LEFT JOIN tmnf_market.goods g ON p.goods_id = g.id
    LEFT JOIN tmnf_market.goods_plan gp ON p.goods_plan_id = gp.id
    LEFT JOIN tmnf_customer.party_channel pchannel ON pchannel.party_id = p.holder_customer_id
    LEFT JOIN tmnf_customer.party_customer pcustomer_holder ON pcustomer_holder.party_id = p.holder_customer_id
    LEFT JOIN tmnf_customer.party_customer_phone pcp_holder ON pcp_holder.party_id = p.holder_customer_id
    LEFT JOIN tmnf_customer.party_customer_email pce_holder ON pce_holder.party_id = p.holder_customer_id
    LEFT JOIN tmnf_customer.party_customer_address pca_holder ON pca_holder.party_id = p.holder_customer_id
    LEFT JOIN tmnf_policy.policy_insurant pi ON p.id = pi.policy_id AND pi.policy_product_id = pp_medical.id
    LEFT JOIN tmnf_customer.party_customer pcustomer_insurant ON pcustomer_insurant.party_id = pi.customer_id
    LEFT JOIN tmnf_customer.party_customer_email pce_insurant ON pce_insurant.party_id = pi.customer_id

WHERE p.goods_id = 727419350695938
ORDER BY p.id DESC
LIMIT 1000;