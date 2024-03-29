ColumnAggregation = {
    "zomato": {
        "txn_metrics": {
            "acceptance_time_tm": "mean",
            "adt_tm": "mean",
            "aov_tm": "mean",
            "commissionable_amt_tm": "sum",
            "delivery_rating_tm": "mean",
            "discounted_orders_tm": "sum",
            "food_order_ready_at_tm": "mean",
            "food_rating_tm": "mean",
            "kpt_actual_new_tm": "mean",
            "la_orders_tm": "sum",
            "mm_orders_tm": "sum",
            "mvd_tm": "sum",
            "mx_rejection_tm": "sum",
            "mx_rejects_subtotal_tm": "sum",
            "new_user_tm": "sum",
            "orders_tm": "sum",
            "overall_discount_value_tm": "sum",
            "pc_tm": "sum",
            "pro_discount_tm": "sum",
            "rejection_tm": "sum",
            "repeat_user_tm": "sum",
            "rider_wait_time_tm": "mean",
            "salt_tm": "sum",
            "subtotal_tm": "sum",
            "timeouts_tm": "sum",
            "total_rejects_subtotal_tm": "sum",
            "total_user_tm": "sum",
            "um_orders_tm": "sum"
        },
        "funnel": {
            "cart_built_funnel": "sum",
            "orders_funnel": "sum",
            "menu_opens_funnel": "sum"
        },
        "grid": {
            "actuals_grid": "sum",
            "expected_grid": "sum"
        },
        "ors": {
            "orders_ors": "sum",
            "ors_ors": "sum",
            "poor_quality_ors": "sum",
            "order_status_delay_ors": "sum",
            "missing_items_ors": "sum",
            "wrong_order_ors": "sum",
            "order_cancellation_ors": "sum",
            "rejection_ors": "sum",
            "order_spilled_ors": "sum",
            "instructions_not_followed_ors": "sum",
            "instructions_ors": "sum",
            "untagged_ors": "sum",
            "others_ors": "sum"
        },
        "ads": {
            "ad_impression_ads": "sum",
            "inorganic_menu_opens_ads": "sum",
            "ad_orders_ads": "sum",
            "sales_generated_ads": "sum",
            "ads_consumed_ads": "sum",
            "ads_new_users_ads": "sum",
            "cart_built_ads": "sum"
        },
        "promo": {
            "promo_orders_promo": "sum",
            "promo_orders_subtotal_promo": "sum",
            "mvd_promo": "sum"
        },
        "for": {
            "orders_for": "sum",
            "for_accuracy_new_for": "mean",
            "for_compliance_for": "mean",
            "comp_for": "mean",
            "acc_for": "mean"
        },
        "new_user": {
            "res_new_user_nu": "sum",
            "adjusted_m_burn_nu": "sum",
        },
        "pro": {
            "orders_pro": "sum",
            "users_pro": "sum",
            "commissionable_value_pro": "sum",
            "pro_discount_pro": "sum",
            "asv_pro": "sum",
            "breakfast_orders_pro": "sum",
            "lunch_orders_pro": "sum",
            "evening_orders_pro": "sum",
            "dinner_orders_pro": "sum",
            "late_night_orders_pro": "sum",
            "overall_discount_value_pro": "sum",
            "merchant_discount_value_pro": "sum",
            "order_acceptance_time_pro": "mean",
            "delivery_time_pro": "mean"
        }
    },
    "swiggy": {
        "raw_data": {
            "aov_raw_data": "mean",
            "orders_raw_data": "sum",
            "sales_generated_raw_data": "sum",
            "mark_food_ready_accuracy_raw_data": "mean",
            "mark_food_ready_adoption_raw_data": "mean",
            "acceptance_raw_data": "mean",
            "restaurant_cancellations_raw_data": "mean",
            "edits_raw_data": "mean",
            "igcc_raw_data": "mean",
            "prep_time_raw_data": "mean",
            "grid_visibility_raw_data": "mean",
            "average_food_ratings_raw_data": "mean",
            "new_user_orders_raw_data": "sum",
            "new_users_overall_raw_data": "sum",
            "repeat_user_orders_raw_data": "sum",
            "repeat_user_base_raw_data": "sum",
            "overall_repeat_rate_raw_data": "mean",
            "p1_customers_raw_data": "sum",
            "p2_customers_raw_data": "sum",
            "p3_customers_raw_data": "sum",
            "unclassified_raw_data": "sum",
            "ad_impression_raw_data": "sum",
            "menu_opens_raw_data": "mean",
            "cart_builds_raw_data": "sum",
            "bad_order_raw_data": "sum",
            "total_food_issue_raw_data": "sum",
            "quality_issue_raw_data": "sum",
            "quantity_issue_raw_data": "sum",
            "packaging_raw_data": "sum",
            "wrong_item_raw_data": "sum",
            "special_inst_issue_raw_data": "sum",
            "missing_item_raw_data": "sum",
            "swiggyit_orders_raw_data": "sum",
            "swiggyit_burn_raw_data": "sum",
            "jumbo_orders_raw_data": "sum",
            "jumbo_burn_raw_data": "sum",
            "party_orders_raw_data": "sum",
            "b2g1_orders_raw_data": "sum",
            "party_burn_raw_data": "sum",
            "unlimited_orders_raw_data": "sum",
            "unlimited_burn_raw_data": "sum",
            "b2g1_burn_raw_data": "sum",
            "b1g1_orders_raw_data": "sum",
            "b1g1_burn_raw_data": "sum",
            "dotd_steal_deal_orders_raw_data": "sum",
            "dotd_steal_deal_burn_raw_data": "sum",
            "dormant_missed_you_orders_raw_data": "sum",
            "dormant_missed_you_burn_raw_data": "sum",
            "new_customer_try_new_orders_raw_data": "sum",
            "new_customer_try_new_burn_raw_data": "sum",
            "swiggy_one_eo_orders_raw_data": "sum",
            "swiggy_one_eo_burn_raw_data": "sum",
        },
        "ads": {
            "ads_consumed_ads": "sum",
            "inorganic_menu_opens_ads": "sum",
            "ad_impression_ads": "sum",
            "ad_orders_ads": "sum",
            "ads_new_users_ads": "sum",
            "sales_generated_ads": "sum"
        }
    }
}
