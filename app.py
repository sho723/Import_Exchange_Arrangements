import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date
import io
import xlrd
import openpyxl
from typing import Tuple, List, Dict, Optional

# ページ設定
st.set_page_config(
    page_title="輸入為替必要金額整理自動化PoC",
    page_icon="📊",
    layout="wide"
)

def load_excel_file(uploaded_file) -> pd.DataFrame:
    """Excelファイルを読み込む"""
    try:
        if uploaded_file.name.endswith('.xls'):
            # .xlsファイルの場合
            df = pd.read_excel(uploaded_file, engine='xlrd')
        else:
            # .xlsx, .xlsmファイルの場合
            df = pd.read_excel(uploaded_file, engine='openpyxl')
        return df
    except Exception as e:
        st.error(f"ファイル読み込みエラー: {str(e)}")
        return None

def filter_unwanted_records(df: pd.DataFrame, index_col: str = 'IndexNo') -> pd.DataFrame:
    """不要なレコードをフィルターして削除"""
    if index_col not in df.columns:
        st.error(f"列'{index_col}'が見つかりません")
        return df
    
    # 削除条件
    conditions = [
        df[index_col].str.startswith('BAPE', na=False),  # 輸出
        df[index_col].str.endswith('AUIM', na=False),    # 麦
        df[index_col].str.contains('SWAP', na=False),    # SWAP
        df[index_col].str.startswith('BAPG', na=False),  # 諸掛
        df[index_col].str.contains('OSE', na=False)      # OSE為替
    ]
    
    # いずれかの条件に該当するレコードを削除
    delete_mask = pd.concat(conditions, axis=1).any(axis=1)
    filtered_df = df[~delete_mask].copy()
    
    deleted_count = len(df) - len(filtered_df)
    st.info(f"削除されたレコード数: {deleted_count}")
    
    return filtered_df

def sort_dataframe(df: pd.DataFrame, date_col1: str = '締結日', date_col2: str = 'From') -> pd.DataFrame:
    """データフレームを並び替える"""
    # 日付列を確認
    missing_cols = [col for col in [date_col1, date_col2] if col not in df.columns]
    if missing_cols:
        st.error(f"列が見つかりません: {missing_cols}")
        return df
    
    # 日付型に変換
    try:
        df[date_col1] = pd.to_datetime(df[date_col1], errors='coerce')
        df[date_col2] = pd.to_datetime(df[date_col2], errors='coerce')
    except Exception as e:
        st.warning(f"日付変換警告: {str(e)}")
    
    # 並び替え
    sorted_df = df.sort_values([date_col1, date_col2], na_position='last')
    
    return sorted_df

def calculate_payment_allocation(
    df: pd.DataFrame,
    payment_amount: float,
    payment_date: date,
    balance_col: str = '紐づけ後残高',
    from_col: str = 'From'
) -> Dict:
    """支払い金額の配分計算"""
    
    # 支払い期日と同じ月のレコードを抽出
    try:
        df[from_col] = pd.to_datetime(df[from_col], errors='coerce')
        target_month = f"{payment_date.year}-{payment_date.month:02d}"
        
        # 同じ月のレコードを抽出
        same_month_mask = df[from_col].dt.strftime('%Y-%m') == target_month
        target_records = df[same_month_mask].copy()
        
        if target_records.empty:
            return {
                'status': 'no_records',
                'message': f"{target_month}の対象レコードがありません",
                'records': pd.DataFrame(),
                'total_balance': 0,
                'shortage': payment_amount
            }
        
        # 残高を数値型に変換
        target_records[balance_col] = pd.to_numeric(target_records[balance_col], errors='coerce').fillna(0)
        
        # 累積合計を計算
        target_records['累積残高'] = target_records[balance_col].cumsum()
        
        # 支払い金額を超過するレコードを特定
        exceeded_mask = target_records['累積残高'] >= payment_amount
        
        if exceeded_mask.any():
            # 超過するレコードがある場合
            exceed_index = exceeded_mask.idxmax()
            selected_records = target_records.loc[:exceed_index].copy()
            
            total_balance = selected_records[balance_col].sum()
            split_remainder = total_balance - payment_amount
            
            return {
                'status': 'sufficient',
                'message': f"支払い可能（分割残: {split_remainder:,.0f}）",
                'records': selected_records,
                'total_balance': total_balance,
                'split_remainder': split_remainder,
                'payment_amount': payment_amount
            }
        else:
            # 残高が不足している場合
            total_balance = target_records[balance_col].sum()
            shortage = payment_amount - total_balance
            
            return {
                'status': 'insufficient',
                'message': f"残高不足（マリー予約取得必要金額: {shortage:,.0f}）",
                'records': target_records,
                'total_balance': total_balance,
                'shortage': shortage,
                'payment_amount': payment_amount
            }
            
    except Exception as e:
        st.error(f"計算エラー: {str(e)}")
        return {
            'status': 'error',
            'message': f"エラーが発生しました: {str(e)}",
            'records': pd.DataFrame(),
            'total_balance': 0
        }

def create_download_excel(result: Dict) -> bytes:
    """結果をExcelファイルとして出力"""
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # 対象レコード
        if not result['records'].empty:
            result['records'].to_excel(writer, sheet_name='対象レコード', index=False)
        
        # サマリー情報
        summary_data = {
            '項目': ['ステータス', '総残高', '支払い金額'],
            '値': [
                result['status'],
                f"{result['total_balance']:,.0f}",
                f"{result.get('payment_amount', 0):,.0f}"
            ]
        }
        
        if result['status'] == 'sufficient':
            summary_data['項目'].append('分割残')
            summary_data['値'].append(f"{result['split_remainder']:,.0f}")
        elif result['status'] == 'insufficient':
            summary_data['項目'].append('マリー予約取得必要金額')
            summary_data['値'].append(f"{result['shortage']:,.0f}")
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='サマリー', index=False)
    
    output.seek(0)
    return output.read()

def main():
    st.title("📊 輸入為替必要金額整理自動化PoC")
    st.markdown("---")
    
    # サイドバーでの設定
    st.sidebar.header("🔧 設定")
    
    # ファイルアップロード
    uploaded_file = st.sidebar.file_uploader(
        "Excelファイルをアップロード",
        type=['xls', 'xlsx', 'xlsm'],
        help="処理対象のExcelファイルを選択してください"
    )
    
    if uploaded_file is not None:
        # ファイル読み込み
        with st.spinner("ファイルを読み込み中..."):
            df = load_excel_file(uploaded_file)
        
        if df is not None:
            st.success(f"ファイル読み込み完了: {len(df)}件のレコード")
            
            # 列名の確認と設定
            st.sidebar.subheader("📋 列設定")
            
            available_cols = list(df.columns)
            
            index_col = st.sidebar.selectbox(
                "IndexNo列",
                available_cols,
                index=available_cols.index('IndexNo') if 'IndexNo' in available_cols else 0
            )
            
            date_col1 = st.sidebar.selectbox(
                "締結日列",
                available_cols,
                index=available_cols.index('締結日') if '締結日' in available_cols else 0
            )
            
            date_col2 = st.sidebar.selectbox(
                "From列",
                available_cols,
                index=available_cols.index('From') if 'From' in available_cols else 0
            )
            
            balance_col = st.sidebar.selectbox(
                "紐づけ後残高列",
                available_cols,
                index=available_cols.index('紐づけ後残高') if '紐づけ後残高' in available_cols else 0
            )
            
            # 支払い条件設定
            st.sidebar.subheader("💰 支払い条件")
            
            payment_amount = st.sidebar.number_input(
                "支払い金額",
                min_value=0.0,
                value=1000000.0,
                step=10000.0,
                format="%.0f"
            )
            
            payment_date = st.sidebar.date_input(
                "支払い期日",
                value=date.today()
            )
            
            # 処理実行ボタン
            if st.sidebar.button("🚀 処理実行", type="primary"):
                
                # メインエリアでの処理表示
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.header("📈 処理結果")
                    
                    # Step 1: 不要レコード削除
                    st.subheader("Step 1: 不要レコード削除")
                    with st.spinner("不要レコードを削除中..."):
                        filtered_df = filter_unwanted_records(df, index_col)
                    
                    # Step 2: 並び替え
                    st.subheader("Step 2: データ並び替え")
                    with st.spinner("データを並び替え中..."):
                        sorted_df = sort_dataframe(filtered_df, date_col1, date_col2)
                    
                    # Step 3: 支払い配分計算
                    st.subheader("Step 3: 支払い配分計算")
                    with st.spinner("支払い配分を計算中..."):
                        result = calculate_payment_allocation(
                            sorted_df, payment_amount, payment_date, balance_col, date_col2
                        )
                    
                    # 結果表示
                    if result['status'] == 'sufficient':
                        st.success(result['message'])
                        st.metric(
                            "分割残",
                            f"{result['split_remainder']:,.0f}円",
                            delta=f"総残高: {result['total_balance']:,.0f}円"
                        )
                    elif result['status'] == 'insufficient':
                        st.warning(result['message'])
                        st.metric(
                            "マリー予約取得必要金額",
                            f"{result['shortage']:,.0f}円",
                            delta=f"現在残高: {result['total_balance']:,.0f}円"
                        )
                    else:
                        st.error(result['message'])
                    
                    # 対象レコード表示
                    if not result['records'].empty:
                        st.subheader("📋 対象レコード一覧")
                        st.dataframe(result['records'], use_container_width=True)
                        
                        # ダウンロードボタン
                        excel_data = create_download_excel(result)
                        st.download_button(
                            label="📥 結果をExcelでダウンロード",
                            data=excel_data,
                            file_name=f"処理結果_{payment_date.strftime('%Y%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                
                with col2:
                    st.header("📊 統計情報")
                    
                    # 処理統計
                    st.metric("元レコード数", len(df))
                    st.metric("処理後レコード数", len(sorted_df))
                    st.metric("削除レコード数", len(df) - len(sorted_df))
                    
                    # 支払い条件
                    st.subheader("💰 支払い条件")
                    st.info(f"**支払い金額**: {payment_amount:,.0f}円")
                    st.info(f"**支払い期日**: {payment_date}")
                    st.info(f"**対象月**: {payment_date.strftime('%Y年%m月')}")
            
            # データプレビュー
            st.header("👀 データプレビュー")
            if st.checkbox("元データを表示"):
                st.dataframe(df.head(10), use_container_width=True)
    
    else:
        st.info("👆 サイドバーからExcelファイルをアップロードしてください")
        
        # 使用方法の説明
        st.header("📖 使用方法")
        st.markdown("""
        ### 処理手順
        1. **ファイルアップロード**: .xls/.xlsx/.xlsmファイルを選択
        2. **列設定**: データの列名を確認・設定
        3. **支払い条件**: 支払い金額と支払い期日を入力
        4. **処理実行**: 自動処理を開始
        5. **結果確認**: 処理結果とレコード一覧を確認
        6. **ダウンロード**: 結果をExcelファイルでダウンロード
        
        ### 処理内容
        - 不要レコードの自動削除（BAPE、AUIM、SWAP、BAPG、OSE）
        - 締結日・From列での並び替え
        - 支払い金額に基づく自動配分計算
        - 分割残・不足金額の自動算出
        """)

if __name__ == "__main__":
    main()
